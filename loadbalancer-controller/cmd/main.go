/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

// loadbalancer-controller watches Service objects of type LoadBalancer
// across every Cozystack KubernetesSwitchcloud tenant cluster and
// provisions a matching Octavia LB in the underlying Switch Cloud
// OpenStack project. The controller lives in the management cluster so
// that tenant cluster users never need to handle OpenStack credentials
// themselves — they just declare `type: LoadBalancer` and an IP is
// returned.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/controller"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/ksc"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/multicluster"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/openstack"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/restart"
)

var scheme = runtime.NewScheme()

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "loadbalancer-controller: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var opts struct {
		metricsAddr          string
		probeAddr            string
		enableLeaderElection bool
		zapOpts              zap.Options
	}

	flag.StringVar(&opts.metricsAddr, "metrics-bind-address", ":8080",
		"The address the metrics endpoint binds to. Use 0 to disable.")
	flag.StringVar(&opts.probeAddr, "health-probe-bind-address", ":8081",
		"The address the probe endpoint binds to.")
	flag.BoolVar(&opts.enableLeaderElection, "leader-elect", false,
		"Enable leader election for controller manager.")
	opts.zapOpts.BindFlags(flag.CommandLine)
	flag.Parse()

	logger := zap.New(zap.UseFlagOptions(&opts.zapOpts))
	ctrl.SetLogger(logger)

	cfg := ctrl.GetConfigOrDie()

	ctx, cancel := context.WithCancel(ctrl.SetupSignalHandler())
	defer cancel()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: opts.metricsAddr},
		HealthProbeBindAddress: opts.probeAddr,
		LeaderElection:         opts.enableLeaderElection,
		LeaderElectionID:       "9b27e8c3.loadbalancer.switchcloud.aenix.io",
		// Scope the management-cluster cache to tenant-root only.
		// Every mgmt-side resource the controller reads (KubernetesSwitchcloud
		// CRs, openstack credential Secrets) lives there. Without this scope
		// controller-runtime spins up cluster-wide informers for Secrets and
		// the ServiceAccount's namespace-bound Role can't satisfy them, which
		// floods the log with RBAC denials and blocks reconciliation.
		Cache: cache.Options{
			DefaultNamespaces: map[string]cache.Config{
				ksc.TenantNamespace: {},
			},
		},
	})
	if err != nil {
		return errors.Wrap(err, "creating manager")
	}

	registry, err := multicluster.Build(ctx, cfg, logger.WithName("registry"))
	if err != nil {
		return errors.Wrap(err, "building tenant cluster registry")
	}

	// Start each tenant cluster cache in its own goroutine instead of
	// going through mgr.Add. controller-runtime treats Add'd Runnables
	// as blocking — a single unreachable tenant (offline apiserver,
	// stale kubeconfig, transient network blip during startup) would
	// otherwise crash-loop the entire centralised controller and break
	// every other tenant. Isolating the lifecycle here keeps the blast
	// radius scoped to that one tenant. The shared context still
	// cancels them all on SIGTERM.
	for name, c := range registry.All() {
		go runTenantCluster(ctx, name, c, logger)
	}

	r := &controller.ServiceReconciler{
		Registry:   registry,
		MgmtClient: mgr.GetClient(),
		Log:        logger.WithName("controller"),
	}

	if err := r.SetupWithManager(mgr); err != nil {
		return errors.Wrap(err, "registering Service reconciler")
	}

	clusterReconciler := &controller.ClusterReconciler{
		MgmtClient: mgr.GetClient(),
		Log:        logger.WithName("cluster"),
	}

	if err := clusterReconciler.SetupWithManager(mgr); err != nil {
		return errors.Wrap(err, "registering Cluster reconciler")
	}

	// Periodic orphan sweeper — picks up OpenStack resources that
	// outlived their owning Service / KSC CR because the controller
	// happened to be down or somebody mutated state out-of-band. The
	// CR-finalizer + per-Service finalizer are the primary cleanup
	// paths; this is the safety net.
	go runOrphanSweeper(ctx, mgr.GetClient(), logger.WithName("sweeper"))

	// Tenant-secret watcher. The multicluster.Registry is built once
	// against the kubeconfig Secrets visible at startup, and the
	// per-tenant cluster.Cluster objects it holds are frozen for the
	// life of the manager (controller-runtime does not let us mutate
	// Watches after mgr.Start). If a tenant is created, deleted, or
	// recreated with a fresh Kamaji CA, the registry would silently
	// keep dialling the wrong endpoint and flood the log with x509
	// errors. The watcher fingerprints the secret set at startup,
	// then on every Secret event recomputes; on drift it cancels the
	// manager context so the pod exits and Kubernetes restarts us
	// against the current state of the world. Same pattern as
	// kilo-clustermesh-operator's restart.ChangeWatcher.
	// Snapshot the tenant Secret set via a fresh uncached client (the
	// manager cache only serves reads after mgr.Start, which has not
	// happened yet) and pass it to the watcher as StartFingerprint.
	// Any drift observed by the watcher after this point is real
	// movement since startup, not a cache-warmup race.
	preStartClient, err := ctrlclient.New(cfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return errors.Wrap(err, "building uncached client for startup fingerprint")
	}

	startFingerprint, err := restart.ComputeFingerprint(ctx, preStartClient)
	if err != nil {
		return errors.Wrap(err, "computing tenant-secret startup fingerprint")
	}

	tenantWatcher := &restart.TenantSecretWatcher{
		Client:           mgr.GetClient(),
		Cancel:           cancel,
		StartFingerprint: startFingerprint,
		Log:              logger.WithName("tenant-secret-watcher"),
	}

	if err := tenantWatcher.SetupWithManager(mgr); err != nil {
		return errors.Wrap(err, "registering tenant-secret watcher")
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return errors.Wrap(err, "setting up healthz")
	}

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return errors.Wrap(err, "setting up readyz")
	}

	logger.Info("starting manager", "tenants", registry.Names())

	if err := mgr.Start(ctx); err != nil {
		return errors.Wrap(err, "manager exited with error")
	}

	return nil
}

// sweepInterval is the cadence of the orphan sweeper. Long-ish on
// purpose — primary cleanup is the per-reconcile finalizer path; this
// loop is the safety net for "controller was down when somebody
// deleted something" cases.
const sweepInterval = 10 * time.Minute

// runOrphanSweeper periodically asks OpenStack for the full list of
// our managed resources and compares it against the set of clusters
// we currently know about. Anything whose owning cluster has gone
// missing is torn down. Survives transient OpenStack auth failures —
// errors are logged and the loop continues; the next tick retries.
func runOrphanSweeper(ctx context.Context, mgmtClient ctrlclient.Client, log logr.Logger) {
	t := time.NewTicker(sweepInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		// Source of truth for "live cluster" = the HelmRelease that
		// cozystack-api generates from the KubernetesSwitchcloud CR.
		// Using the kubeconfig-Secret-derived registry would be
		// circular here: those Secrets are exactly the artifacts we
		// want to clean up when their owning cluster is gone.
		known, err := listKnownClustersFromHRs(ctx, mgmtClient)
		if err != nil {
			log.Error(err, "sweep: listing live KSC HelmReleases failed")

			continue
		}

		if len(known) == 0 {
			// No live clusters known yet (early startup, or genuinely
			// empty mgmt cluster) — skip rather than nuke everything
			// that looks like it might be orphaned.
			continue
		}

		// Pick any known cluster to authenticate as. All clusters in
		// the same project share creds in the typical Switch Cloud
		// layout — clouds.yaml lookups resolve identically. If
		// projects diverge we'd need to iterate per-cluster; not the
		// case today.
		var anyCluster string
		for n := range known {
			anyCluster = n

			break
		}

		cfg, err := ksc.Resolve(ctx, mgmtClient, anyCluster)
		if err != nil {
			log.Error(err, "sweep: resolving KSC config for auth failed", "cluster", anyCluster)

			continue
		}

		// Sweeper must run even when the picked cluster's LB feature
		// is opted out: orphan resources we're trying to delete come
		// from clusters that no longer exist or that previously had
		// the feature on. All we need from the picked cluster is
		// valid OpenStack credentials.
		if cfg.Creds.AuthURL == "" {
			continue
		}

		clients, err := openstack.NewClients(ctx, cfg.Creds)
		if err != nil {
			log.Error(err, "sweep: building OpenStack clients failed")

			continue
		}

		// Nova first: workers reference the cluster SG via their
		// ports, so the SG delete inside SweepOrphans returns 409
		// while VMs are alive. Starting termination here gives them
		// a head start; the SG sweep below is best-effort and the
		// next cycle picks up whatever was still locked.
		if err := openstack.SweepOrphanNovaServers(ctx, clients, known); err != nil {
			log.Error(err, "sweep cycle failed (Nova layer)")

			continue
		}

		if err := openstack.SweepOrphans(ctx, clients, known); err != nil {
			log.Error(err, "sweep cycle failed (LB/SG layer)")

			continue
		}

		if err := sweepOrphanKubeconfigSecrets(ctx, mgmtClient, known); err != nil {
			log.Error(err, "sweep cycle failed (Kamaji kubeconfig secrets)")

			continue
		}

		log.V(1).Info("sweep cycle complete", "knownClusters", len(known))
	}
}

// listKnownClustersFromHRs returns the set of live cluster names by
// listing every HelmRelease in the tenant-root namespace that
// cozystack-api generated from a KubernetesSwitchcloud CR (label
// apps.cozystack.io/application.kind=KubernetesSwitchcloud). HRs in
// terminating state are excluded — by the time the apiserver removes
// our finalizer the cluster's resources should already be torn down,
// and we don't want a sweep to fight an in-flight teardown.
//
// Informer freshness: this reads through the manager's cache. A
// brand-new HR may not be visible for a few hundred milliseconds
// after cozystack-api admits it. The sweep ticks every 10 minutes,
// so the realistic odds of catching a not-yet-cached HR are tiny;
// the worst-case fallout (deleting a freshly-issued kubeconfig
// Secret that Kamaji then recreates) is one paging round, not data
// loss. The `len(known) == 0` guard above also catches the broader
// "informer hasn't synced anything yet" failure mode.
func listKnownClustersFromHRs(ctx context.Context, mgmtClient ctrlclient.Client) (map[string]struct{}, error) {
	list := &unstructured.UnstructuredList{}
	list.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "helm.toolkit.fluxcd.io",
		Version: "v2",
		Kind:    "HelmReleaseList",
	})

	err := mgmtClient.List(ctx, list,
		ctrlclient.InNamespace(ksc.TenantNamespace),
		ctrlclient.MatchingLabels{"apps.cozystack.io/application.kind": "KubernetesSwitchcloud"},
	)
	if err != nil {
		return nil, errors.Wrap(err, "listing KSC HelmReleases")
	}

	out := make(map[string]struct{}, len(list.Items))

	for i := range list.Items {
		if !list.Items[i].GetDeletionTimestamp().IsZero() {
			continue
		}

		cluster := list.Items[i].GetLabels()["apps.cozystack.io/application.name"]
		if cluster == "" {
			continue
		}

		out[cluster] = struct{}{}
	}

	return out, nil
}

// sweepOrphanKubeconfigSecrets deletes Kamaji-managed
// <release>-admin-kubeconfig Secrets in tenant-root for clusters
// that are no longer in `known`. Kamaji itself is responsible for
// cleaning these up when the TenantControlPlane is removed, but the
// chart's uninstall flow has historically left them behind under
// failure modes (e.g. cozystack-api stripping a finalizer before the
// chart could cascade-delete). Targeting only the deterministic
// naming pattern keeps us from touching unrelated Secrets.
func sweepOrphanKubeconfigSecrets(ctx context.Context, mgmtClient ctrlclient.Client, known map[string]struct{}) error {
	list := &corev1.SecretList{}

	err := mgmtClient.List(ctx, list, ctrlclient.InNamespace(ksc.TenantNamespace))
	if err != nil {
		return errors.Wrap(err, "listing tenant-root secrets")
	}

	const prefix = "kubernetes-switchcloud-"
	const suffix = "-admin-kubeconfig"

	for i := range list.Items {
		name := list.Items[i].Name

		if !strings.HasPrefix(name, prefix) || !strings.HasSuffix(name, suffix) {
			continue
		}

		cluster := strings.TrimSuffix(strings.TrimPrefix(name, prefix), suffix)
		if cluster == "" {
			continue
		}

		if _, alive := known[cluster]; alive {
			continue
		}

		secret := &corev1.Secret{}
		secret.Namespace = list.Items[i].Namespace
		secret.Name = name

		if err := mgmtClient.Delete(ctx, secret); err != nil && !apierrors.IsNotFound(err) {
			return errors.Wrapf(err, "deleting orphan kubeconfig secret %s", name)
		}
	}

	return nil
}

// runTenantCluster runs a single tenant's cluster.Cluster cache. Errors
// (apiserver unreachable, watch failures) are logged but never
// propagated to the manager: this tenant simply produces no Service
// events until the apiserver comes back, while every other tenant
// keeps reconciling. Same pattern as kilo-clustermesh-operator.
func runTenantCluster(ctx context.Context, name string, c cluster.Cluster, log logr.Logger) {
	tenantLog := log.WithValues("tenant", name)

	if err := c.Start(ctx); err != nil && ctx.Err() == nil {
		// ctx.Err() == nil means the cancellation is coming from the
		// cluster itself, not from a parent shutdown signal — log it
		// as a genuine tenant failure rather than expected teardown.
		tenantLog.Error(err, "tenant cluster cache exited with error")
	}
}
