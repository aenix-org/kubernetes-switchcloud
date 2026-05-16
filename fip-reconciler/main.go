package main

import (
	"context"
	"flag"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

func main() {
	var (
		kubeconfigPath string
		cloudsYAMLPath string
		cloudName      string
		interval       time.Duration
		namespace      string
	)

	flag.StringVar(&kubeconfigPath, "kubeconfig", "", "path to kubeconfig (uses in-cluster config if empty)")
	flag.StringVar(&cloudsYAMLPath, "clouds-yaml", "/etc/openstack/clouds.yaml", "path to OpenStack clouds.yaml")
	flag.StringVar(&cloudName, "cloud-name", "openstack", "cloud name in clouds.yaml")
	flag.DurationVar(&interval, "interval", 30*time.Second, "polling interval")
	flag.StringVar(&namespace, "namespace", "", "namespace to watch (falls back to POD_NAMESPACE env)")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if namespace == "" {
		namespace = os.Getenv("POD_NAMESPACE")
	}

	var cfg *rest.Config
	var err error
	if kubeconfigPath != "" {
		cfg, err = clientcmd.BuildConfigFromFlags("", kubeconfigPath)
	} else {
		cfg, err = rest.InClusterConfig()
	}
	if err != nil {
		log.Error("failed to build kubeconfig", "err", err)
		os.Exit(1)
	}

	dynClient, err := dynamic.NewForConfig(cfg)
	if err != nil {
		log.Error("failed to create dynamic client", "err", err)
		os.Exit(1)
	}

	r := &Reconciler{
		kube:           dynClient,
		cloudsYAMLPath: cloudsYAMLPath,
		cloudName:      cloudName,
		namespace:      namespace,
		log:            log,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer cancel()

	log.Info("starting fip-reconciler", "namespace", namespace, "interval", interval)

	// Run immediately on startup, then on each tick.
	if err := r.Reconcile(ctx); err != nil {
		log.Error("initial reconcile failed", "err", err)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := r.Reconcile(ctx); err != nil {
				log.Error("reconcile failed", "err", err)
			}
		case <-ctx.Done():
			log.Info("shutting down")
			return
		}
	}
}
