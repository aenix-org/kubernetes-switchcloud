/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
*/

// loadbalancer-controller watches Service objects of type LoadBalancer
// across every Cozystack KubernetesSwitchcloud tenant cluster and (in
// later phases) provisions a matching Octavia LB in the underlying
// Switch Cloud OpenStack project. The controller lives in the
// management cluster so that tenant cluster users never need to handle
// OpenStack credentials themselves — they just declare
// `type: LoadBalancer` and an IP is returned.
//
// This file is the scaffold for v0: it boots a controller-runtime
// manager, builds a multi-tenant registry keyed by KubernetesSwitchcloud
// CR, attaches a Service watcher to every tenant's cluster.Cluster, and
// logs LoadBalancer Service events. No OpenStack calls yet — that ships
// in v1.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"

	"github.com/cockroachdb/errors"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/controller"
	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/multicluster"
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

	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&opts.zapOpts)))
	slogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	cfg := ctrl.GetConfigOrDie()

	ctx, cancel := context.WithCancel(ctrl.SetupSignalHandler())
	defer cancel()

	mgr, err := ctrl.NewManager(cfg, ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: opts.metricsAddr},
		HealthProbeBindAddress: opts.probeAddr,
		LeaderElection:         opts.enableLeaderElection,
		LeaderElectionID:       "9b27e8c3.loadbalancer.switchcloud.aenix.io",
	})
	if err != nil {
		return errors.Wrap(err, "creating manager")
	}

	registry, err := multicluster.Build(ctx, cfg, slogger)
	if err != nil {
		return errors.Wrap(err, "building tenant cluster registry")
	}

	for name, c := range registry.All() {
		if err := mgr.Add(c); err != nil {
			return errors.Wrapf(err, "registering tenant cluster %q with manager", name)
		}
	}

	r := &controller.ServiceReconciler{
		Registry: registry,
		Log:      slogger,
	}

	if err := r.SetupWithManager(mgr); err != nil {
		return errors.Wrap(err, "registering Service reconciler")
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		return errors.Wrap(err, "setting up healthz")
	}

	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		return errors.Wrap(err, "setting up readyz")
	}

	slogger.Info("Starting manager",
		slog.String("tenants", fmt.Sprintf("%v", registry.Names())),
	)

	if err := mgr.Start(ctx); err != nil {
		return errors.Wrap(err, "manager exited with error")
	}

	return nil
}
