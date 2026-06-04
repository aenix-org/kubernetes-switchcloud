/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

// Package controller wires Service-watch reconcilers, one per
// discovered tenant cluster, into the controller-runtime manager.
//
// On every Service event we resolve the tenant's KubernetesSwitchcloud
// CR, short-circuit when the loadBalancer feature is opted out, and
// otherwise call into internal/openstack to ensure the matching
// Octavia LB + listeners + pool + members. The reconciler then
// patches the Service.status.loadBalancer.ingress back into the
// tenant cluster so kube-proxy / kubectl get svc shows the assigned
// VIP. A finalizer guarantees we get a chance to delete the LB even
// when the Service is removed or its type is changed away from
// LoadBalancer.
package controller

import (
	"context"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/ksc"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/multicluster"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/openstack"
)

// FinalizerName is added to every Service the controller is responsible
// for. Its presence is the signal that there may be an Octavia LB out
// there that needs cleaning up before the Service object can disappear
// from the apiserver — covers both Service deletion and type changes
// away from LoadBalancer (where the spec we'd need to compute the LB
// name is otherwise lost).
const FinalizerName = "loadbalancer.switchcloud.aenix.io/cleanup"

// ServiceReconciler attaches a controller-runtime controller to every
// tenant's cluster.Cluster so that each tenant gets an independent
// Service watch and work queue.
type ServiceReconciler struct {
	Registry   *multicluster.Registry
	MgmtClient client.Client
	Log        logr.Logger
}

// SetupWithManager registers one controller per tenant. Each tenant's
// controller has its own queue, its own informer (backed by the
// tenant's cluster.Cache), and reconciles only Services within that
// tenant. Cross-tenant blast radius is therefore bounded.
func (r *ServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	for tenant, c := range r.Registry.All() {
		tenantReconciler := &tenantServiceReconciler{
			tenant:       tenant,
			tenantClient: c.GetClient(),
			mgmtClient:   r.MgmtClient,
			log:          r.Log.WithValues("tenant", tenant),
		}

		err := ctrl.NewControllerManagedBy(mgr).
			Named("service-" + tenant).
			WatchesRawSource(source.Kind(c.GetCache(), &corev1.Service{}, &handler.TypedEnqueueRequestForObject[*corev1.Service]{})).
			Complete(tenantReconciler)
		if err != nil {
			return errors.Wrapf(err, "registering Service controller for tenant %q", tenant)
		}
	}

	return nil
}

// memberResyncAfter is the requeue interval used whenever the LB is
// already in place — gives us periodic member-list resync without
// having to plumb a second informer for tenant Nodes. Short enough
// that scale events recover within a minute, long enough that an idle
// controller doesn't burn API calls.
const memberResyncAfter = 60 * time.Second

// pendingRequeue is used while Octavia is mid-operation
// (PENDING_CREATE / PENDING_UPDATE / PENDING_DELETE). We yield the
// worker rather than block it; the next reconcile picks up where this
// one left off when the LB becomes ACTIVE.
const pendingRequeue = 5 * time.Second

type tenantServiceReconciler struct {
	tenant       string
	tenantClient client.Client
	mgmtClient   client.Client
	log          logr.Logger
}

func (r *tenantServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	svc := &corev1.Service{}

	err := r.tenantClient.Get(ctx, req.NamespacedName, svc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			// Object already gone (e.g. after we removed our finalizer
			// in a previous reconcile). Nothing to do.
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "fetching Service")
	}

	// Fast path: Service that is neither type=LoadBalancer nor carries
	// our finalizer is none of our business. Most Services in a tenant
	// cluster (kubernetes, kube-dns, in-cluster operators, …) fall
	// here. Returning before ksc.Resolve avoids both unnecessary
	// management-cluster API calls and noisy errors when the tenant's
	// loadBalancer config is intentionally minimal.
	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer && !containsString(svc.Finalizers, FinalizerName) {
		return ctrl.Result{}, nil
	}

	// Resolve config once. Cleanup paths need it too, so do it up
	// front. If the CR itself is gone we treat the controller as
	// disabled for this tenant — same as opted-out.
	cfg, err := ksc.Resolve(ctx, r.mgmtClient, r.tenant)
	if err != nil {
		if ksc.IsNotFound(err) {
			return r.dropFinalizer(ctx, svc)
		}

		return ctrl.Result{}, errors.Wrap(err, "resolving KubernetesSwitchcloud config")
	}

	// Deletion + type-change cleanup both flow through here. The
	// finalizer is the contract that lets us hold the Service object
	// open long enough to delete the matching Octavia LB.
	managed := svc.Spec.Type == corev1.ServiceTypeLoadBalancer && cfg.Enabled
	beingDeleted := !svc.DeletionTimestamp.IsZero()

	if !managed && cfg.MisconfiguredReason != "" && svc.Spec.Type == corev1.ServiceTypeLoadBalancer {
		// Operator opted in but the config is incomplete. Log once
		// at Warning level and stop — no requeue, the next CR
		// update will re-trigger reconcile via the management
		// cluster watch (out of scope here; for now operators must
		// rely on the controller noticing on the next Service
		// event after they fix the CR).
		r.log.Info("loadBalancer feature misconfigured; skipping Service",
			"namespace", svc.Namespace,
			"name", svc.Name,
			"reason", cfg.MisconfiguredReason,
		)

		return ctrl.Result{}, nil
	}

	if !managed || beingDeleted {
		return r.cleanup(ctx, svc, cfg)
	}

	// Service is a LoadBalancer and feature is on — make sure the
	// finalizer is set before we provision anything OpenStack-side.
	if !containsString(svc.Finalizers, FinalizerName) {
		patched := svc.DeepCopy()
		patched.Finalizers = append(patched.Finalizers, FinalizerName)

		if err := r.tenantClient.Patch(ctx, patched, client.MergeFrom(svc)); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "adding finalizer")
		}

		// The patch updated resourceVersion; let the watch deliver
		// the next event so we work against a fresh object.
		return ctrl.Result{}, nil
	}

	clients, err := openstack.NewClients(ctx, cfg.Creds)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "building OpenStack clients")
	}

	memberSubnetID, err := openstack.ResolveMemberSubnet(ctx, clients, cfg.VIPNetworkID)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "resolving member subnet")
	}

	lb, pending, err := openstack.EnsureLB(ctx, clients, r.tenant, svc, cfg.VIPNetworkID, cfg.ProviderDriver)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "ensuring Octavia LB")
	}

	if pending {
		// LB still mid-operation. Don't block the worker; let it
		// settle on the next reconcile.
		r.log.V(1).Info("LB still pending; requeueing",
			"namespace", svc.Namespace,
			"name", svc.Name,
		)

		return ctrl.Result{RequeueAfter: pendingRequeue}, nil
	}

	// Member list reflects current Ready Nodes. Empty is a legitimate
	// state (zero-scale tenant, all nodes NotReady) — feed it through
	// SyncListenersAndMembers so stale members are cleared. We still
	// patch the VIP onto status regardless.
	memberIPs, err := r.tenantNodeIPs(ctx)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "listing tenant node IPs")
	}

	if pending, err := openstack.SyncListenersAndMembers(ctx, clients, lb, svc, memberIPs, memberSubnetID); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "syncing listeners and members")
	} else if pending {
		return ctrl.Result{RequeueAfter: pendingRequeue}, nil
	}

	// Decide the externally-visible address. When floatingNetworkID
	// is configured the LB's VIP is internal (tenant-network) and the
	// FIP is what tenant users hit; otherwise we publish the VIP
	// itself (useful for IPv6-only or internal-only setups).
	publicAddr := lb.VipAddress

	if cfg.FloatingNetworkID != "" {
		fipDesc := openstack.ServiceLBName(r.tenant, svc.Namespace, svc.Name)

		fipAddr, err := openstack.EnsureFloatingIP(ctx, clients, lb.VipPortID, cfg.FloatingNetworkID, cfg.FloatingSubnetID, fipDesc)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "ensuring floating IP")
		}

		publicAddr = fipAddr
	}

	if err := r.patchStatus(ctx, svc, publicAddr); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "patching Service status")
	}

	r.log.Info("Service reconciled",
		"namespace", svc.Namespace,
		"name", svc.Name,
		"vip", lb.VipAddress,
		"publicAddress", publicAddr,
		"members", len(memberIPs),
	)

	return ctrl.Result{RequeueAfter: memberResyncAfter}, nil
}

// cleanup deletes the Octavia LB (if any) for this Service and then
// removes the finalizer so the apiserver can finish deletion. Runs on
// both Service deletion and Service.spec.type changing away from
// LoadBalancer (so we don't leak OpenStack resources). Idempotent.
func (r *tenantServiceReconciler) cleanup(ctx context.Context, svc *corev1.Service, cfg *ksc.LoadBalancerConfig) (ctrl.Result, error) {
	if !containsString(svc.Finalizers, FinalizerName) {
		// Never claimed by us — nothing to clean up.
		return ctrl.Result{}, nil
	}

	if cfg.Enabled {
		// Feature is enabled for this tenant, so an LB may exist.
		clients, err := openstack.NewClients(ctx, cfg.Creds)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "building OpenStack clients for cleanup")
		}

		pending, err := openstack.DeleteLB(ctx, clients, r.tenant, svc)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "deleting Octavia LB")
		}

		if pending {
			// LB is in PENDING_DELETE — requeue and check again rather
			// than dropping the finalizer prematurely.
			return ctrl.Result{RequeueAfter: pendingRequeue}, nil
		}
	}

	return r.dropFinalizer(ctx, svc)
}

func (r *tenantServiceReconciler) dropFinalizer(ctx context.Context, svc *corev1.Service) (ctrl.Result, error) {
	if !containsString(svc.Finalizers, FinalizerName) {
		return ctrl.Result{}, nil
	}

	patched := svc.DeepCopy()
	patched.Finalizers = removeString(patched.Finalizers, FinalizerName)

	if err := r.tenantClient.Patch(ctx, patched, client.MergeFrom(svc)); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "removing finalizer")
	}

	r.log.Info("finalizer removed",
		"namespace", svc.Namespace,
		"name", svc.Name,
	)

	return ctrl.Result{}, nil
}

// tenantNodeIPs returns the InternalIPs of every Ready tenant Node.
// Returning the empty slice is fine — callers must be able to handle
// "no Ready nodes" without skipping pool sync (otherwise stale members
// leak when a tenant scales to zero).
func (r *tenantServiceReconciler) tenantNodeIPs(ctx context.Context) ([]string, error) {
	var nodes corev1.NodeList

	if err := r.tenantClient.List(ctx, &nodes); err != nil {
		return nil, errors.Wrap(err, "listing tenant Nodes")
	}

	ips := make([]string, 0, len(nodes.Items))

	for i := range nodes.Items {
		n := &nodes.Items[i]

		if !nodeReady(n) {
			continue
		}

		for _, addr := range n.Status.Addresses {
			if addr.Type == corev1.NodeInternalIP && addr.Address != "" {
				ips = append(ips, addr.Address)

				break
			}
		}
	}

	return ips, nil
}

func nodeReady(n *corev1.Node) bool {
	for _, c := range n.Status.Conditions {
		if c.Type == corev1.NodeReady {
			return c.Status == corev1.ConditionTrue
		}
	}

	return false
}

// patchStatus writes back the Octavia VIP into
// Service.status.loadBalancer.ingress. Uses a server-side merge patch
// so that other status writers (controllers, kubelet) keep their
// fields untouched.
func (r *tenantServiceReconciler) patchStatus(ctx context.Context, svc *corev1.Service, vip string) error {
	if vip == "" {
		return errors.New("OpenStack LB returned empty VipAddress")
	}

	for _, ing := range svc.Status.LoadBalancer.Ingress {
		if ing.IP == vip {
			return nil
		}
	}

	// Refetch the Service to get a clean status base (avoids racing
	// with kube-controller-manager / kubelet who also touch status).
	fresh := &corev1.Service{}
	if err := r.tenantClient.Get(ctx, types.NamespacedName{Namespace: svc.Namespace, Name: svc.Name}, fresh); err != nil {
		return errors.Wrap(err, "refetching Service before status patch")
	}

	patch := client.MergeFrom(fresh.DeepCopy())
	fresh.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: vip}}

	if err := r.tenantClient.Status().Patch(ctx, fresh, patch); err != nil {
		return errors.Wrap(err, "patching Service status loadBalancer.ingress")
	}

	return nil
}

func containsString(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}

	return false
}

func removeString(slice []string, s string) []string {
	out := slice[:0]
	for _, v := range slice {
		if v != s {
			out = append(out, v)
		}
	}

	return out
}
