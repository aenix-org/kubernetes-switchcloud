/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

// Package restart watches the set of tenant kubeconfig Secrets in the
// management cluster and triggers a controlled process exit when that
// set changes shape (a tenant appears, disappears, or has its CA
// rotated by a delete+recreate cycle). The exit is the signal for
// Kubernetes to restart the pod, at which point the static
// multicluster.Registry is rebuilt against the current Secrets and
// every per-tenant controller is wired up against the right CA.
//
// Background: controller-runtime's controller is registered at
// SetupWithManager time and cannot have its Watches mutated after
// mgr.Start(). The cluster.Cluster a tenant controller dials through
// is therefore frozen for the manager's lifetime — if a tenant's
// Kamaji control plane is destroyed and re-provisioned, the new
// apiserver presents a cert signed by a brand-new CA, and the old
// REST client in the registry keeps emitting "x509: certificate
// signed by unknown authority" on every list/watch forever. Same
// problem in reverse for a removed tenant: nothing is there to
// dial, but the controller keeps trying.
//
// kilo-clustermesh-operator solved the same shape by watching the
// inputs that the static configuration was derived from and
// cancelling the manager context on change. Mirror that pattern
// here: cheap to implement, no surprises, and the restart story is
// the one Kubernetes operators already understand.
package restart

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/cockroachdb/errors"
	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
)

// These must match multicluster.tenantNamespace / kubeconfigNamePrefix
// / kubeconfigSuffix. Duplicated locally rather than exported from
// internal/multicluster to keep the dependency direction one-way
// (multicluster depends on no other internal package) and to make the
// watcher self-contained for code review.
const (
	tenantNamespace      = "tenant-root"
	kubeconfigNamePrefix = "kubernetes-switchcloud-"
	kubeconfigSuffix     = "-admin-kubeconfig"
)

// TenantSecretWatcher watches the set of tenant kubeconfig Secrets in
// the management cluster. When the fingerprint of that set diverges
// from StartFingerprint it invokes Cancel, which propagates through
// the manager and exits the process. Kubernetes restarts the pod and
// the next process build sees the current tenant set from scratch.
type TenantSecretWatcher struct {
	client.Client

	Cancel           context.CancelFunc
	StartFingerprint string
	Log              logr.Logger
}

// SetupWithManager registers the watcher with the manager. Watches
// only Secrets in tenant-root whose name matches the kubeconfig
// convention; everything else (Helm release Secrets, image-pull
// secrets, …) is filtered out before it ever lands on the work queue.
func (w *TenantSecretWatcher) SetupWithManager(mgr ctrl.Manager) error {
	err := ctrl.NewControllerManagedBy(mgr).
		Named("tenant-secret-watcher").
		For(&corev1.Secret{}, builder.WithPredicates(predicate.NewPredicateFuncs(isTenantKubeconfigSecret))).
		Complete(w)
	if err != nil {
		return errors.Wrap(err, "building tenant-secret-watcher controller")
	}

	return nil
}

// Reconcile recomputes the fingerprint and triggers a restart if it
// drifted from the startup snapshot. The Reconcile body is the same
// regardless of which Secret fired — any change in the tenant set is
// reason enough to rebuild, and recomputing over the full list keeps
// the watcher resilient to missed events.
func (w *TenantSecretWatcher) Reconcile(ctx context.Context, _ reconcile.Request) (reconcile.Result, error) {
	fp, err := w.ComputeFingerprint(ctx)
	if err != nil {
		return reconcile.Result{}, errors.Wrap(err, "computing tenant-secret fingerprint")
	}

	if fp == w.StartFingerprint {
		return reconcile.Result{}, nil
	}

	w.Log.Info("tenant kubeconfig set changed; cancelling manager to trigger restart",
		"old", w.StartFingerprint,
		"new", fp,
	)

	if w.Cancel != nil {
		w.Cancel()
	}

	return reconcile.Result{}, nil
}

// ComputeFingerprint returns a deterministic hash of the current
// tenant kubeconfig set. The hash inputs are the Secret name and the
// ResourceVersion: ResourceVersion changes on any Update (including
// CA rotation after delete+recreate), and a removed/added tenant is
// captured by the name list itself.
//
// Exported so main can call it once at startup to seed
// StartFingerprint before the manager starts the controllers — the
// controllers see the same snapshot the watcher was initialised with,
// and the first reconcile that hits a different fingerprint really
// reflects a change since startup, not a startup race.
func (w *TenantSecretWatcher) ComputeFingerprint(ctx context.Context) (string, error) {
	var secrets corev1.SecretList
	if err := w.List(ctx, &secrets, client.InNamespace(tenantNamespace)); err != nil {
		return "", errors.Wrap(err, "listing tenant Secrets")
	}

	type ref struct {
		name string
		rv   string
	}

	refs := make([]ref, 0, len(secrets.Items))

	for i := range secrets.Items {
		s := &secrets.Items[i]
		if !isTenantKubeconfigName(s.Name) {
			continue
		}

		refs = append(refs, ref{name: s.Name, rv: s.ResourceVersion})
	}

	sort.Slice(refs, func(i, j int) bool { return refs[i].name < refs[j].name })

	h := sha256.New()

	for _, r := range refs {
		h.Write([]byte(r.name))
		h.Write([]byte{0})
		h.Write([]byte(r.rv))
		h.Write([]byte{0})
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// isTenantKubeconfigName reports whether a Secret name follows the
// `kubernetes-switchcloud-<tenant>-admin-kubeconfig` convention.
// Pulled out so the predicate and the fingerprint list agree on the
// filter.
func isTenantKubeconfigName(name string) bool {
	if !strings.HasPrefix(name, kubeconfigNamePrefix) || !strings.HasSuffix(name, kubeconfigSuffix) {
		return false
	}

	tenant := strings.TrimSuffix(strings.TrimPrefix(name, kubeconfigNamePrefix), kubeconfigSuffix)

	return tenant != ""
}

// isTenantKubeconfigSecret is the event-time predicate. Returns true
// when the Secret name matches the kubeconfig convention and the
// namespace is the tenant-root namespace. Applied to Create, Update,
// Delete and Generic events alike — any of those is enough to change
// the fingerprint.
func isTenantKubeconfigSecret(obj client.Object) bool {
	if obj.GetNamespace() != tenantNamespace {
		return false
	}

	return isTenantKubeconfigName(obj.GetName())
}

