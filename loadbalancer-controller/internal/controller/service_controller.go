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
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
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

		// KSC watch: tenants flip loadBalancer.enabled / allowedCIDRs /
		// floatingNetworkID by patching the KSC CR in the management
		// cluster. The tenant-side Service controller would otherwise
		// not see those changes until the next event fires on a Service
		// itself — meaning an operator who flipped enabled=false would
		// leave the Octavia LB up indefinitely until they happened to
		// touch the Service. Translate every KSC event for *this*
		// tenant into reconcile requests for every Service in the
		// tenant's cluster, with finalizer-carriers prioritised so the
		// cleanup pass runs first.
		kscObj := &unstructured.Unstructured{}
		kscObj.SetGroupVersionKind(ksc.GroupVersionKind())

		kscHandler := handler.TypedEnqueueRequestsFromMapFunc[*unstructured.Unstructured](func(ctx context.Context, obj *unstructured.Unstructured) []reconcile.Request {
			if obj == nil || obj.GetName() != tenant {
				return nil
			}

			var list corev1.ServiceList
			if err := tenantReconciler.tenantClient.List(ctx, &list); err != nil {
				tenantReconciler.log.Error(err, "listing Services on KSC change")

				return nil
			}

			reqs := make([]reconcile.Request, 0, len(list.Items))

			for i := range list.Items {
				svc := &list.Items[i]
				if svc.Spec.Type != corev1.ServiceTypeLoadBalancer && !containsString(svc.Finalizers, FinalizerName) {
					continue
				}

				reqs = append(reqs, reconcile.Request{NamespacedName: types.NamespacedName{
					Namespace: svc.Namespace,
					Name:      svc.Name,
				}})
			}

			return reqs
		})

		err := ctrl.NewControllerManagedBy(mgr).
			Named("service-" + tenant).
			WatchesRawSource(source.Kind(c.GetCache(), &corev1.Service{}, &handler.TypedEnqueueRequestForObject[*corev1.Service]{})).
			WatchesRawSource(source.Kind(mgr.GetCache(), kscObj, kscHandler)).
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

	// HR-delete guard. When the tenant's HelmRelease in the management
	// cluster has been marked for deletion, the cluster controller is
	// already running SweepClusterResources and is the source of truth
	// for OpenStack-side cleanup of this tenant. If we let the service
	// controller continue past this point, two reconciles ran in
	// parallel — the cluster sweep deleted the cluster SG + cluster-scoped
	// resources, and then a per-Service Reconcile re-created an LB+FIP
	// that the now-orphan SG couldn't anchor — leaving stranded FIPs
	// in OpenStack that only orphan-sweeper would eventually mop up.
	//
	// Drop our finalizer here so the Service can finish its own
	// deletion when the tenant cluster goes away, but do not run
	// per-Service cleanup or create-path: the cluster sweep covers
	// both, and racing it produces exactly the resource-recreation
	// loop above.
	hrDeleting, err := r.isHelmReleaseDeleting(ctx)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "checking HelmRelease delete state")
	}

	if hrDeleting {
		// Cluster sweep handles the controller-managed SG and the LBs
		// owned by this tenant. If the operator pinned
		// loadBalancer.workerSecurityGroupID to an SG they own,
		// SweepClusterResources will not touch it: that SG outlives
		// the tenant and would otherwise accumulate per-Service
		// NodePort rules tagged `cozystack:<tenant>/<ns>/<svc>:*`
		// from every cluster that ever lived. Best-effort wipe the
		// rules we put there before dropping the finalizer; failures
		// are logged but never block the finalizer-drop, because
		// holding the Service open against a tenant that is going
		// away helps nobody.
		if cfg.WorkerSecurityGroupID != "" && containsString(svc.Finalizers, FinalizerName) {
			if clients, ccErr := openstack.NewClients(ctx, cfg.Creds); ccErr == nil {
				if delErr := openstack.DeleteNodePortRules(ctx, clients, cfg.WorkerSecurityGroupID, r.tenant, svc); delErr != nil {
					r.log.Info("HR terminating: best-effort NodePort rule cleanup failed on operator-supplied SG",
						"namespace", svc.Namespace,
						"name", svc.Name,
						"workerSecurityGroupID", cfg.WorkerSecurityGroupID,
						"error", delErr.Error(),
					)
				}
			} else {
				r.log.Info("HR terminating: skipping operator-SG cleanup, OpenStack client unavailable",
					"namespace", svc.Namespace,
					"name", svc.Name,
					"error", ccErr.Error(),
				)
			}
		}

		r.log.V(1).Info("HelmRelease is terminating; deferring to cluster-level sweep",
			"namespace", svc.Namespace,
			"name", svc.Name,
		)

		return r.dropFinalizer(ctx, svc)
	}

	// Deletion + type-change cleanup both flow through here. The
	// finalizer is the contract that lets us hold the Service object
	// open long enough to delete the matching Octavia LB.
	managed := svc.Spec.Type == corev1.ServiceTypeLoadBalancer && cfg.Enabled
	beingDeleted := !svc.DeletionTimestamp.IsZero()

	if !managed && cfg.MisconfiguredReason != "" && svc.Spec.Type == corev1.ServiceTypeLoadBalancer && !containsString(svc.Finalizers, FinalizerName) {
		// Operator opted in but the config is incomplete, and we
		// never claimed this Service — nothing to do. Log once and
		// stop. The next CR update will re-trigger reconcile via
		// the management cluster watch. A misconfigured Service
		// that DOES carry our finalizer falls through to the
		// cleanup branch below: if cfg.Creds resolved we can still
		// tear down any LB we left behind (e.g. operator broke the
		// config after the LB was created, then deleted the
		// Service — we must not hold the Service hostage).
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

	// Member list = OpenStack-side worker IPs on the cluster network,
	// not Node.status.addresses[InternalIP]. kubelet may report a
	// CNI/overlay address (Kilo WireGuard) as InternalIP which OVN
	// has no path to, so a Neutron-derived list is the only thing
	// the LB can actually reach. Empty is still a legitimate state
	// (no workers yet) — feed it through SyncListenersAndMembers so
	// stale members are cleared. We still patch the VIP onto status.
	memberIPs, err := openstack.ListWorkerIPsOnNetwork(ctx, clients, cfg.VIPNetworkID)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "listing worker IPs from OpenStack")
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

	// Auto-discover the FIP source network when the operator left
	// the field empty: pick the project's single external network
	// (router:external=true). Matches the Switch Cloud zhw `public`
	// topology by default and keeps the CR minimal — operator only
	// needs to set `enabled: true` plus credentials. An operator who
	// wants an internal-only LB sets `floatingNetworkID: none` (or
	// any future sentinel) — for now empty == auto-discover.
	floatingNetID := cfg.FloatingNetworkID
	if floatingNetID == "" {
		discovered, err := openstack.FindFirstExternalNetwork(ctx, clients)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "auto-discovering floating network")
		}

		floatingNetID = discovered
	}

	if floatingNetID != "" {
		fipDesc := openstack.ServiceLBName(r.tenant, svc.Namespace, svc.Name)

		fipAddr, err := openstack.EnsureFloatingIP(ctx, clients, lb.VipPortID, floatingNetID, cfg.FloatingSubnetID, fipDesc)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "ensuring floating IP")
		}

		publicAddr = fipAddr
	}

	// Resolve which SG carries the NodePort rules. Operator override
	// via cfg.WorkerSecurityGroupID wins; otherwise the controller
	// auto-creates a per-cluster SG (cozystack-lb-<cluster>) with the
	// intra-SG baseline rules in place. Distinct SGs across clusters
	// give us L4 isolation by default — co-tenant clusters share the
	// project but not the SG, so allow-from-same-SG never crosses
	// cluster boundaries.
	workerSGID := cfg.WorkerSecurityGroupID
	controllerManagedSG := false

	if workerSGID == "" {
		sgID, err := openstack.EnsureClusterSecurityGroup(ctx, clients, r.tenant)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "ensuring cluster security group")
		}

		workerSGID = sgID
		controllerManagedSG = true
	}

	// When the controller manages the SG it also owns attaching it to
	// every worker port. Operator-supplied SGs are assumed to already
	// be wired into the workers via CAPI's nodeGroups.securityGroups —
	// touching their port attachments would race CAPI's reconciler.
	if controllerManagedSG {
		if err := openstack.EnsureSGAttachedToWorkers(ctx, clients, workerSGID, memberIPs, cfg.VIPNetworkID); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "attaching cluster SG to worker ports")
		}

		// Strip the project default SG from worker ports so its
		// implicit allow-from-same-default-SG rule can no longer
		// carry traffic across clusters that share the project. Must
		// run after EnsureSGAttachedToWorkers — that step guarantees
		// the cluster SG is present, so detach never leaves a port
		// with zero SGs.
		if err := openstack.DetachDefaultSGFromWorkers(ctx, clients, memberIPs, cfg.VIPNetworkID); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "detaching default SG from worker ports")
		}
	}

	if err := openstack.EnsureNodePortRules(ctx, clients, workerSGID, r.tenant, svc, cfg.AllowedCIDRs); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "ensuring nodePort SG rules")
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

	// The presence of our finalizer is the source of truth that we
	// created OpenStack-side resources for this Service in the past.
	// Gating cleanup on cfg.Enabled would mean: operator flips
	// loadBalancer.enabled false on a tenant that still has live LB
	// Services, we drop the finalizer immediately, and the Octavia
	// LBs / FIPs / SG rules get orphaned in OpenStack. So we run
	// cleanup unconditionally and only fall through to dropFinalizer
	// once the OpenStack side reports clean.
	clients, err := openstack.NewClients(ctx, cfg.Creds)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "building OpenStack clients for cleanup")
	}

	// Drop NodePort SG rules first — cheap to delete and tidier this
	// way. SG resolution mirrors the create path: operator override
	// if present, otherwise the controller's per-cluster SG (looked
	// up by deterministic name; do NOT auto-create on cleanup).
	cleanupSGID, err := r.resolveCleanupSGID(ctx, clients, cfg)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "resolving cleanup SG")
	}

	if cleanupSGID != "" {
		if err := openstack.DeleteNodePortRules(ctx, clients, cleanupSGID, r.tenant, svc); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "deleting nodePort SG rules")
		}
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

	// Clear Service.status.loadBalancer.ingress before dropping the
	// finalizer. Otherwise kubectl get svc keeps showing the (now
	// dead) FIP and any external DNS A-record pointing at that
	// status silently turns into a black hole. Standard cloud-provider
	// convention is to publish on create and clear on delete; the
	// previous behaviour only honoured the publish half.
	if err := r.clearStatusIngress(ctx, svc); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "clearing Service status loadBalancer.ingress")
	}

	return r.dropFinalizer(ctx, svc)
}

// resolveCleanupSGID returns the SG ID rules should be removed from
// during cleanup. Mirrors the create-path resolution but never
// creates: a missing controller-managed SG means there are no rules
// to clean up. Operator override always wins.
func (r *tenantServiceReconciler) resolveCleanupSGID(ctx context.Context, clients *openstack.Clients, cfg *ksc.LoadBalancerConfig) (string, error) {
	if cfg.WorkerSecurityGroupID != "" {
		return cfg.WorkerSecurityGroupID, nil
	}

	sg, err := openstack.LookupClusterSecurityGroup(ctx, clients, r.tenant)
	if err != nil {
		return "", err
	}

	return sg, nil
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

// isHelmReleaseDeleting reports whether the tenant's HelmRelease in
// the management cluster is in its terminating phase. We look up the
// HR by deterministic name `kubernetes-switchcloud-<tenant>` rather
// than by label so the lookup is cheap (no list, no informer scan)
// and reuses the management-cluster cache that the controller already
// has. A missing HR returns true as well: the tenant cluster is on
// its way out one way or another, and producing fresh OpenStack
// resources for it would be guaranteed orphan-making.
func (r *tenantServiceReconciler) isHelmReleaseDeleting(ctx context.Context) (bool, error) {
	hr := &unstructured.Unstructured{}
	hr.SetGroupVersionKind(helmReleaseGVK)

	err := r.mgmtClient.Get(ctx, types.NamespacedName{
		Namespace: ksc.TenantNamespace,
		Name:      "kubernetes-switchcloud-" + r.tenant,
	}, hr)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return true, nil
		}

		return false, errors.Wrap(err, "fetching HelmRelease")
	}

	return hr.GetDeletionTimestamp() != nil, nil
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

// clearStatusIngress wipes Service.status.loadBalancer.ingress when
// the underlying Octavia LB has been torn down. Mirrors patchStatus
// in shape (refetch + merge-patch on status subresource) but writes
// an empty slice. No-op when there is nothing to clear so we don't
// generate spurious API server traffic on every cleanup pass.
func (r *tenantServiceReconciler) clearStatusIngress(ctx context.Context, svc *corev1.Service) error {
	if len(svc.Status.LoadBalancer.Ingress) == 0 {
		return nil
	}

	fresh := &corev1.Service{}
	if err := r.tenantClient.Get(ctx, types.NamespacedName{Namespace: svc.Namespace, Name: svc.Name}, fresh); err != nil {
		if apierrors.IsNotFound(err) {
			// Service already gone — nothing to clear.
			return nil
		}

		return errors.Wrap(err, "refetching Service before clearing status")
	}

	if len(fresh.Status.LoadBalancer.Ingress) == 0 {
		return nil
	}

	patch := client.MergeFrom(fresh.DeepCopy())
	fresh.Status.LoadBalancer.Ingress = nil

	if err := r.tenantClient.Status().Patch(ctx, fresh, patch); err != nil {
		return errors.Wrap(err, "patching Service status loadBalancer.ingress to empty")
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
