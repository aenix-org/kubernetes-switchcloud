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
	"time"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/runtime"
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
	go runOrphanSweeper(ctx, mgr.GetClient(), registry, logger.WithName("sweeper"))

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
func runOrphanSweeper(ctx context.Context, mgmtClient ctrlclient.Client, reg *multicluster.Registry, log logr.Logger) {
	t := time.NewTicker(sweepInterval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}

		known := make(map[string]struct{}, len(reg.Names()))
		for _, n := range reg.Names() {
			known[n] = struct{}{}
		}

		if len(known) == 0 {
			// Registry not yet populated (early startup) — skip a
			// cycle rather than sweep against an empty set, which
			// would look like every resource is orphaned.
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

		if err := openstack.SweepOrphans(ctx, clients, known); err != nil {
			log.Error(err, "sweep cycle failed")

			continue
		}

		log.V(1).Info("sweep cycle complete", "knownClusters", len(known))
	}
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
