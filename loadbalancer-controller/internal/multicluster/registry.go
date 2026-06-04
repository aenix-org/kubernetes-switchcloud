/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

// Package multicluster builds and holds a controller-runtime
// cluster.Cluster for every Cozystack KubernetesSwitchcloud tenant
// discovered in the management cluster. The registry is the boundary
// between "find which tenants exist and how to dial them" and "actually
// watch their resources" — the rest of the controller talks to tenants
// through this opaque registry.
package multicluster

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
)

const (
	tenantNamespace      = "tenant-root"
	kubeconfigSuffix     = "-admin-kubeconfig"
	kubeconfigSecretKey  = "super-admin.conf"
	kubeconfigNamePrefix = "kubernetes-switchcloud-"
)

// Registry holds one cluster.Cluster per tenant, keyed by tenant name.
// The tenant name is the short form (e.g. "mesh1"), derived from the
// kubeconfig Secret name `kubernetes-switchcloud-<tenant>-admin-kubeconfig`.
type Registry struct {
	clusters map[string]cluster.Cluster
}

// Build discovers tenants in the management cluster and constructs a
// Registry. Per-tenant failures (missing/unparseable kubeconfig,
// cluster.New error) are logged and skipped — they do not abort the
// build. Same pattern as kilo-clustermesh-operator: a partial registry
// is safe because the rest of the controller best-efforts every
// tenant independently.
func Build(ctx context.Context, mgmtCfg *rest.Config, log logr.Logger) (*Registry, error) {
	scheme := runtime.NewScheme()
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))

	pre, err := ctrlclient.New(mgmtCfg, ctrlclient.Options{Scheme: scheme})
	if err != nil {
		return nil, errors.Wrap(err, "building management-cluster client")
	}

	var secrets corev1.SecretList
	if err := pre.List(ctx, &secrets, ctrlclient.InNamespace(tenantNamespace)); err != nil {
		return nil, errors.Wrap(err, "listing tenant kubeconfig Secrets")
	}

	reg := &Registry{clusters: make(map[string]cluster.Cluster)}

	for i := range secrets.Items {
		s := &secrets.Items[i]
		if !strings.HasPrefix(s.Name, kubeconfigNamePrefix) || !strings.HasSuffix(s.Name, kubeconfigSuffix) {
			continue
		}

		tenant := strings.TrimSuffix(strings.TrimPrefix(s.Name, kubeconfigNamePrefix), kubeconfigSuffix)
		if tenant == "" {
			continue
		}

		kubeconfig, ok := s.Data[kubeconfigSecretKey]
		if !ok || len(kubeconfig) == 0 {
			log.Info("tenant kubeconfig Secret missing super-admin.conf; skipping",
				"tenant", tenant,
				"secret", s.Name,
			)

			continue
		}

		cfg, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
		if err != nil {
			log.Error(err, "tenant kubeconfig unparseable; skipping",
				"tenant", tenant,
			)

			continue
		}

		c, err := cluster.New(cfg, func(o *cluster.Options) {
			o.Scheme = scheme
		})
		if err != nil {
			log.Error(err, "cluster.New failed for tenant; skipping",
				"tenant", tenant,
			)

			continue
		}

		reg.clusters[tenant] = c
	}

	return reg, nil
}

// All returns every tenant's cluster.Cluster keyed by tenant name.
func (r *Registry) All() map[string]cluster.Cluster {
	return r.clusters
}

// Names lists discovered tenant names in arbitrary order. Used for log
// output at controller startup.
func (r *Registry) Names() []string {
	names := make([]string, 0, len(r.clusters))
	for n := range r.clusters {
		names = append(names, n)
	}

	return names
}

// Cluster returns the named tenant's cluster.Cluster if it exists.
func (r *Registry) Cluster(name string) (cluster.Cluster, bool) {
	c, ok := r.clusters[name]

	return c, ok
}
