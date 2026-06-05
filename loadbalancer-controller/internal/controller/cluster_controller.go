/*
Copyright 2026 The Aenix Authors.
*/

package controller

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/ksc"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/openstack"
)

// ClusterFinalizerName ensures the controller gets a chance to tear
// down all OpenStack resources it provisioned for a cluster before
// the KubernetesSwitchcloud CR is allowed to disappear. Per-Service
// finalizers run inside the tenant cluster; this one runs in the
// management cluster against the CR itself so it works even after
// the tenant apiserver is gone.
const ClusterFinalizerName = "loadbalancer.switchcloud.aenix.io/cluster-cleanup"

// ClusterReconciler watches KubernetesSwitchcloud CRs in the
// management cluster. On every reconcile it ensures the CR carries
// our finalizer; when the CR enters terminating state it runs
// SweepClusterResources (deletes the LBs + cluster SG owned by this
// cluster) and only then removes the finalizer so the apiserver can
// proceed with deletion.
type ClusterReconciler struct {
	MgmtClient client.Client
	Log        logr.Logger
}

var kscGVK = schema.GroupVersionKind{
	Group:   "apps.cozystack.io",
	Version: "v1alpha1",
	Kind:    "KubernetesSwitchcloud",
}

func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	u := &unstructured.Unstructured{}
	u.SetGroupVersionKind(kscGVK)

	err := ctrl.NewControllerManagedBy(mgr).
		Named("cluster").
		For(u).
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "registering Cluster controller")
	}

	return nil
}

func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	obj := &unstructured.Unstructured{}
	obj.SetGroupVersionKind(kscGVK)

	if err := r.MgmtClient.Get(ctx, req.NamespacedName, obj); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "fetching KubernetesSwitchcloud")
	}

	cluster := obj.GetName()
	log := r.Log.WithValues("cluster", cluster)

	if obj.GetDeletionTimestamp().IsZero() {
		// Live CR — ensure finalizer is present so we get one last
		// reconcile when it transitions to terminating.
		if containsString(obj.GetFinalizers(), ClusterFinalizerName) {
			return ctrl.Result{}, nil
		}

		patched := obj.DeepCopy()
		patched.SetFinalizers(append(patched.GetFinalizers(), ClusterFinalizerName))

		if err := r.MgmtClient.Patch(ctx, patched, client.MergeFrom(obj)); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "adding cluster finalizer")
		}

		return ctrl.Result{}, nil
	}

	// Terminating — run cleanup if our finalizer is still on the CR.
	if !containsString(obj.GetFinalizers(), ClusterFinalizerName) {
		return ctrl.Result{}, nil
	}

	cfg, err := ksc.Resolve(ctx, r.MgmtClient, cluster)
	if err != nil {
		if ksc.IsNotFound(err) {
			// Already gone — defensive, shouldn't really happen here
			// since we just Got the same CR. Drop finalizer to unblock.
			return r.dropClusterFinalizer(ctx, obj)
		}

		return ctrl.Result{}, errors.Wrap(err, "resolving KSC config during deletion")
	}

	if cfg.Enabled || cfg.MisconfiguredReason != "" {
		// Either feature is on (normal case) or was misconfigured but
		// might have leaked OpenStack resources earlier — sweep either
		// way. Credentials are still in the (terminating) CR so we
		// can authenticate.
		clients, err := openstack.NewClients(ctx, cfg.Creds)
		if err != nil {
			return ctrl.Result{}, errors.Wrap(err, "building OpenStack clients for cluster sweep")
		}

		if err := openstack.SweepClusterResources(ctx, clients, cluster); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "sweeping cluster resources")
		}

		log.Info("cluster resources swept")
	}

	return r.dropClusterFinalizer(ctx, obj)
}

func (r *ClusterReconciler) dropClusterFinalizer(ctx context.Context, obj *unstructured.Unstructured) (ctrl.Result, error) {
	if !containsString(obj.GetFinalizers(), ClusterFinalizerName) {
		return ctrl.Result{}, nil
	}

	patched := obj.DeepCopy()
	patched.SetFinalizers(removeString(patched.GetFinalizers(), ClusterFinalizerName))

	if err := r.MgmtClient.Patch(ctx, patched, client.MergeFrom(obj)); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "removing cluster finalizer")
	}

	r.Log.Info("cluster finalizer removed", "cluster", obj.GetName())

	return ctrl.Result{}, nil
}
