/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

// Package controller wires Service-watch reconcilers, one per discovered
// tenant cluster, into the controller-runtime manager. v0 only logs
// events for Service objects of type LoadBalancer; the actual Octavia
// provisioning lands in v1.
package controller

import (
	"context"
	"log/slog"

	"github.com/cockroachdb/errors"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/source"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/multicluster"
)

// ServiceReconciler attaches a controller-runtime controller to every
// tenant cluster.Cluster so that each tenant gets an independent
// Service watch and work queue. v0 simply logs LoadBalancer Service
// events; v1 will replace the log with Octavia CRUD.
type ServiceReconciler struct {
	Registry *multicluster.Registry
	Log      *slog.Logger
}

// SetupWithManager registers one controller per tenant. Each tenant's
// controller has its own queue, its own informer (backed by the
// tenant's cluster.Cache), and reconciles only Services within that
// tenant. Cross-tenant blast radius is therefore bounded.
func (r *ServiceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	for tenant, c := range r.Registry.All() {
		tenantReconciler := &tenantServiceReconciler{
			tenant: tenant,
			client: c.GetClient(),
			log:    r.Log.With(slog.String("tenant", tenant)),
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

// tenantServiceReconciler is the per-tenant reconcile entry. v0
// behaviour: fetch the Service and log when it is type LoadBalancer.
// Non-LB Services are silently ignored — we never want to act on
// ClusterIP / NodePort / ExternalName.
type tenantServiceReconciler struct {
	tenant string
	client client.Client
	log    *slog.Logger
}

// Reconcile handles every Service event in the watched tenant.
func (r *tenantServiceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	svc := &corev1.Service{}

	if err := r.client.Get(ctx, req.NamespacedName, svc); err != nil {
		if client.IgnoreNotFound(err) == nil {
			r.log.Info("Service deleted",
				slog.String("namespace", req.Namespace),
				slog.String("name", req.Name),
			)
			// v1: cascade-delete the corresponding Octavia LB here.

			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "fetching Service")
	}

	if svc.Spec.Type != corev1.ServiceTypeLoadBalancer {
		return ctrl.Result{}, nil
	}

	r.log.Info("LoadBalancer Service observed",
		slog.String("namespace", svc.Namespace),
		slog.String("name", svc.Name),
		slog.String("clusterIP", svc.Spec.ClusterIP),
		slog.Int("ports", len(svc.Spec.Ports)),
		slog.Bool("hasIngressIP", len(svc.Status.LoadBalancer.Ingress) > 0),
	)
	// v1: ensure Octavia LB exists, listener/pool/members synced, then
	// patch svc.Status.LoadBalancer.Ingress with the VIP.

	return ctrl.Result{}, nil
}
