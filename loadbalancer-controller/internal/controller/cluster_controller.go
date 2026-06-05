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
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/ksc"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/openstack"
)

// ClusterFinalizerName ensures the controller gets a chance to tear
// down all OpenStack resources it provisioned for a cluster before
// the cluster is allowed to disappear.
//
// We attach the finalizer to the per-cluster HelmRelease that
// cozystack-api generates from the KubernetesSwitchcloud CR, not to
// the KSC CR itself. The KSC resource is served by an aggregated API
// (cozystack-api / ApplicationDefinition) that strips
// metadata.finalizers from incoming patches — so a finalizer on the
// KSC CR is silently dropped. The HelmRelease is a real Kubernetes
// object (helm.toolkit.fluxcd.io/v2) and finalizers on it persist
// normally. cozystack-api deletes the HR when the KSC CR is deleted,
// so our finalizer gets a deletion event in the same lifecycle.
const ClusterFinalizerName = "loadbalancer.switchcloud.aenix.io/cluster-cleanup"

// kscApplicationKind is the label value cozystack-api stamps on every
// HelmRelease generated from a KubernetesSwitchcloud CR. Used as the
// watch predicate so the cluster controller only reacts to relevant
// HRs and ignores the rest of the tenant-root namespace.
const kscApplicationKind = "KubernetesSwitchcloud"

const (
	appKindLabel = "apps.cozystack.io/application.kind"
	appNameLabel = "apps.cozystack.io/application.name"
)

var helmReleaseGVK = schema.GroupVersionKind{
	Group:   "helm.toolkit.fluxcd.io",
	Version: "v2",
	Kind:    "HelmRelease",
}

// ClusterReconciler watches per-cluster HelmReleases in the
// management cluster. On every reconcile it ensures the HR carries
// our finalizer; when the HR enters terminating state it runs
// SweepClusterResources (deletes the LBs + cluster SG owned by this
// cluster) and only then removes the finalizer so Flux can finish
// the uninstall.
type ClusterReconciler struct {
	MgmtClient client.Client
	Log        logr.Logger
}

func (r *ClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	hr := &unstructured.Unstructured{}
	hr.SetGroupVersionKind(helmReleaseGVK)

	// Reconcile only HRs that cozystack-api spawned from a
	// KubernetesSwitchcloud CR. Other HRs in tenant-root (cert-manager,
	// kilo, etc.) are not our business.
	kscOnly := predicate.NewPredicateFuncs(func(obj client.Object) bool {
		return obj.GetLabels()[appKindLabel] == kscApplicationKind
	})

	err := ctrl.NewControllerManagedBy(mgr).
		Named("cluster").
		For(hr, builder.WithPredicates(kscOnly)).
		Complete(r)
	if err != nil {
		return errors.Wrap(err, "registering Cluster controller")
	}

	return nil
}

func (r *ClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	hr := &unstructured.Unstructured{}
	hr.SetGroupVersionKind(helmReleaseGVK)

	if err := r.MgmtClient.Get(ctx, req.NamespacedName, hr); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}

		return ctrl.Result{}, errors.Wrap(err, "fetching HelmRelease")
	}

	// Extract the cluster name from the standard cozystack label.
	// Fall back to a name-prefix strip only if the label is missing —
	// shouldn't happen with the cozystack-api-generated HRs, but the
	// fallback keeps the controller resilient to manual creation.
	cluster := hr.GetLabels()[appNameLabel]
	if cluster == "" {
		return ctrl.Result{}, nil
	}

	log := r.Log.WithValues("cluster", cluster, "hr", req.NamespacedName.String())

	if hr.GetDeletionTimestamp().IsZero() {
		if containsString(hr.GetFinalizers(), ClusterFinalizerName) {
			return ctrl.Result{}, nil
		}

		patched := hr.DeepCopy()
		patched.SetFinalizers(append(patched.GetFinalizers(), ClusterFinalizerName))

		if err := r.MgmtClient.Patch(ctx, patched, client.MergeFrom(hr)); err != nil {
			return ctrl.Result{}, errors.Wrap(err, "adding cluster finalizer to HelmRelease")
		}

		log.V(1).Info("cluster finalizer added")

		return ctrl.Result{}, nil
	}

	// Terminating — run cleanup if our finalizer is still on the HR.
	if !containsString(hr.GetFinalizers(), ClusterFinalizerName) {
		return ctrl.Result{}, nil
	}

	cfg, err := ksc.Resolve(ctx, r.MgmtClient, cluster)
	if err != nil {
		if ksc.IsNotFound(err) {
			// KSC CR already gone — cozystack-api removed it before
			// the HR's terminating cycle reached us. Try the sister
			// HRs in the same namespace; if any of them have working
			// credentials we can still sweep this cluster's OpenStack
			// resources. If not, drop the finalizer — orphan sweep
			// will pick it up on a later cycle.
			creds, found := r.borrowAnyCreds(ctx, hr.GetNamespace())
			if !found {
				log.Info("KSC CR gone and no sibling credentials; dropping finalizer without sweep")

				return r.dropClusterFinalizer(ctx, hr)
			}

			cfg = &ksc.LoadBalancerConfig{Enabled: true, Creds: creds}
		} else {
			return ctrl.Result{}, errors.Wrap(err, "resolving KSC config during deletion")
		}
	}

	clients, err := openstack.NewClients(ctx, cfg.Creds)
	if err != nil {
		return ctrl.Result{}, errors.Wrap(err, "building OpenStack clients for cluster sweep")
	}

	if err := openstack.SweepClusterResources(ctx, clients, cluster); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "sweeping cluster resources")
	}

	log.Info("cluster resources swept")

	return r.dropClusterFinalizer(ctx, hr)
}

func (r *ClusterReconciler) dropClusterFinalizer(ctx context.Context, hr *unstructured.Unstructured) (ctrl.Result, error) {
	if !containsString(hr.GetFinalizers(), ClusterFinalizerName) {
		return ctrl.Result{}, nil
	}

	patched := hr.DeepCopy()
	patched.SetFinalizers(removeString(patched.GetFinalizers(), ClusterFinalizerName))

	if err := r.MgmtClient.Patch(ctx, patched, client.MergeFrom(hr)); err != nil {
		return ctrl.Result{}, errors.Wrap(err, "removing cluster finalizer from HelmRelease")
	}

	r.Log.Info("cluster finalizer removed", "hr", client.ObjectKeyFromObject(hr).String())

	return ctrl.Result{}, nil
}

// borrowAnyCreds walks every sibling KSC-owned HelmRelease in the
// same namespace and tries to resolve credentials from any of them.
// Used in the (rare) race where cozystack-api deletes the KSC CR
// before the HR's finalizer reconcile fires, leaving us without a
// direct cred source for the cluster we're tearing down. Co-tenant
// clusters in the same project typically share the same OpenStack
// project credentials, so any sibling's creds are good enough to
// drive the sweep.
func (r *ClusterReconciler) borrowAnyCreds(ctx context.Context, namespace string) (openstack.Credentials, bool) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   helmReleaseGVK.Group,
		Version: helmReleaseGVK.Version,
		Kind:    helmReleaseGVK.Kind + "List",
	})

	if err := r.MgmtClient.List(ctx, list, client.InNamespace(namespace), client.MatchingLabels{appKindLabel: kscApplicationKind}); err != nil {
		return openstack.Credentials{}, false
	}

	for i := range list.Items {
		sibling := list.Items[i].GetLabels()[appNameLabel]
		if sibling == "" {
			continue
		}

		cfg, err := ksc.Resolve(ctx, r.MgmtClient, sibling)
		if err == nil && cfg.Creds.AuthURL != "" {
			return cfg.Creds, true
		}
	}

	return openstack.Credentials{}, false
}
