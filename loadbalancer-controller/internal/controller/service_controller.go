/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

// Package controller wires Service-watch reconcilers, one per
// discovered tenant cluster, into the controller-runtime manager.
//
// v1: on every Service event we resolve the tenant's
// KubernetesSwitchcloud CR, short-circuit when the loadBalancer
// feature is opted out, and otherwise call into internal/openstack to
// ensure the matching Octavia LB + listeners + pool + members. The
// reconciler then patches the Service.status.loadBalancer.ingress
// back into the tenant cluster so kube-proxy / kubectl get svc shows
// the assigned VIP.
package controller

import (
	"context"
	"log/slog"
	"time"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/ksc"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/multicluster"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/openstack"
)

// ServiceReconciler attaches a controller-runtime controller to every
// tenant's cluster.Cluster so that each tenant gets an independent
// Service watch and work queue. v1 dispatches reconciles into the
// OpenStack package.
type ServiceReconciler struct {
	Registry   *multicluster.Registry
	MgmtClient client.Client
	Log        *slog.Logger
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
			log:          r.Log.With(slog.String("tenant", tenant)),
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

type tenantServiceReconciler struct {
	tenant       string
	tenantClient client.Client
	mgmtClient   client.Client
	log          *slog.Logger
}

func (r *tenantServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	svc := &corev1.Service{}

	err := r.tenantClient.Get(ctx, req.NamespacedName, svc)
	if err != nil {
		if apierrors.IsNotFound(err) {
			return r.reconcileDeleted(ctx, req)
		}

		return ctrl.Result{}, errors.Wrap(err, "fetching Service")
	}

	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		// Type changed away from LoadBalancer — make sure we don't
		// leave a paid OpenStack LB behind from a previous reconcile.
		return r.reconcileDeleted(ctx, req)
	}

	cfg, err := ksc.Resolve(ctx, r.mgmtClient, r.tenant)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "resolving KubernetesSwitchcloud config")
	}

	if !cfg.Enabled {
		r.log.Debug("loadBalancer disabled on KubernetesSwitchcloud spec; ignoring",
			slog.String("namespace", svc.Namespace),
			slog.String("name", svc.Name),
		)

		return ctrl.Result{}, nil
	}

	clients, err := openstack.NewClients(ctx, cfg.Creds)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "building OpenStack clients")
	}

	vipSubnetID, err := openstack.ResolveVIPSubnet(ctx, clients, cfg.VIPSubnetID)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "resolving VIP subnet")
	}

	lb, err := openstack.EnsureLB(ctx, clients, r.tenant, svc, vipSubnetID, cfg.ProviderDriver)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "ensuring Octavia LB")
	}

	memberIPs, err := r.tenantNodeIPs(ctx)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "listing tenant node IPs")
	}

	if len(memberIPs) == 0 {
		r.log.Warn("no Ready tenant nodes; skipping pool member sync (will requeue)",
			slog.String("namespace", svc.Namespace),
			slog.String("name", svc.Name),
		)

		return ctrl.Result{RequeueAfter: memberResyncAfter}, nil
	}

	if err := openstack.SyncListenersAndMembers(ctx, clients, lb, svc, memberIPs); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "syncing listeners and members")
	}

	if err := r.patchStatus(ctx, svc, lb.VipAddress); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "patching Service status")
	}

	r.log.Info("Service reconciled",
		slog.String("namespace", svc.Namespace),
		slog.String("name", svc.Name),
		slog.String("vip", lb.VipAddress),
		slog.Int("members", len(memberIPs)),
	)

	return ctrl.Result{RequeueAfter: memberResyncAfter}, nil
}

// reconcileDeleted runs when the Service is gone (or no longer a
// LoadBalancer). It removes any matching Octavia LB. Idempotent across
// repeated calls — DeleteLB is a no-op once the LB is gone.
func (r *tenantServiceReconciler) reconcileDeleted(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	cfg, err := ksc.Resolve(ctx, r.mgmtClient, r.tenant)
	if err != nil {
		if ksc.IsNotFound(err) {
			// KubernetesSwitchcloud CR itself is gone — the v2
			// finalizer will own cleanup; nothing to do here.
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "resolving KubernetesSwitchcloud config for delete")
	}

	if !cfg.Enabled {
		return ctrl.Result{}, nil
	}

	clients, err := openstack.NewClients(ctx, cfg.Creds)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "building OpenStack clients for delete")
	}

	dummy := &corev1.Service{}
	dummy.Namespace = req.Namespace
	dummy.Name = req.Name

	if err := openstack.DeleteLB(ctx, clients, r.tenant, dummy); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "deleting Octavia LB")
	}

	r.log.Info("LB deleted",
		slog.String("namespace", req.Namespace),
		slog.String("name", req.Name),
	)

	return ctrl.Result{}, nil
}

// tenantNodeIPs returns the InternalIPs of every Ready tenant Node.
// Used as the Octavia pool's member set: kube-proxy on each node will
// receive nodePort traffic from the LB and DNAT it to the Service's
// backend Pods.
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

	patch := client.MergeFrom(svc.DeepCopy())
	svc.Status.LoadBalancer.Ingress = []corev1.LoadBalancerIngress{{IP: vip}}

	if err := r.tenantClient.Status().Patch(ctx, svc, patch); err != nil {
		return errors.Wrap(err, "patching Service status loadBalancer.ingress")
	}

	return nil
}
