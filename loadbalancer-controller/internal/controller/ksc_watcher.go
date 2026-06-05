/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package controller

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/ksc"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/multicluster"
)

// KSCWatcher is the management-cluster-side controller that watches
// every KubernetesSwitchcloud CR and forwards every change into the
// dynamic tenant Manager. Each KSC event becomes a "re-enqueue every
// LB Service in that tenant's Session" request — so an operator can
// flip spec.openstack.loadBalancer.enabled (or AllowedCIDRs,
// floatingNetworkID, etc.) and have the tenant-side reconciler react
// without having to touch any Service object.
//
// Replaces the static-design WatchesRawSource(KSC) wiring that used
// to live in ServiceReconciler.SetupWithManager. With the dynamic
// Manager the Service controllers come and go at runtime; the KSC
// watch has to live in the mgmt manager (whose controllers are
// stable for the process lifetime) and forward into Sessions by
// tenant name.
type KSCWatcher struct {
	MgmtClient    client.Client
	TenantManager *multicluster.Manager
	Log           logr.Logger
}

// SetupWithManager registers a controller-runtime controller on the
// KubernetesSwitchcloud GVK in tenant-root.
func (w *KSCWatcher) SetupWithManager(mgr ctrl.Manager) error {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(ksc.GroupVersionKind())

	err := ctrl.NewControllerManagedBy(mgr).
		Named("ksc-watcher").
		For(obj).
		Complete(w)
	if err != nil {
		return errors.Wrap(err, "building ksc-watcher controller")
	}

	return nil
}

// Reconcile resolves the KSC name to a tenant and asks the manager
// to enqueue every LB Service in that tenant's Session. The tenant
// reconciler is what actually re-evaluates cfg.Enabled / cleanup /
// create — this handler is just the relay.
func (w *KSCWatcher) Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error) {
	if req.Namespace != ksc.TenantNamespace {
		return reconcile.Result{}, nil
	}

	tenant := req.Name
	if tenant == "" {
		return reconcile.Result{}, nil
	}

	if err := w.TenantManager.EnqueueAllForTenant(ctx, tenant); err != nil {
		return reconcile.Result{}, errors.Wrapf(err, "enqueueing tenant %q after KSC change", tenant)
	}

	return reconcile.Result{}, nil
}
