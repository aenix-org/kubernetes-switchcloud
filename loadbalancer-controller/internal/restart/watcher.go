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
// signed by unknown authority" on every list/watch loop forever.
// Same problem in reverse for a removed tenant: nothing is there
// to dial, but the controller keeps trying.
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

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/multicluster"
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
	fp, err := ComputeFingerprint(ctx, w.Client)
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
// tenant kubeconfig set. Inputs are the Secret name and the
// sha256 of Secret.Data: hashing the payload (rather than
// ResourceVersion) suppresses spurious restarts on label/annotation
// edits, managedFields rewrites, and other apiserver-side metadata
// churn that does not affect what the registry would build. A
// removed/added tenant is captured by the name list itself.
//
// Takes an explicit client.Reader so the caller can supply either the
// manager's cache (post-Start, normal Reconcile path) or a fresh
// uncached client (pre-Start, when seeding StartFingerprint) without
// any temporary state mutation on the watcher.
func ComputeFingerprint(ctx context.Context, c client.Reader) (string, error) {
	var secrets corev1.SecretList
	if err := c.List(ctx, &secrets, client.InNamespace(multicluster.TenantNamespace)); err != nil {
		return "", errors.Wrap(err, "listing tenant Secrets")
	}

	type ref struct {
		name    string
		dataSum [sha256.Size]byte
	}

	refs := make([]ref, 0, len(secrets.Items))

	for i := range secrets.Items {
		s := &secrets.Items[i]
		if !isTenantKubeconfigName(s.Name) {
			continue
		}

		refs = append(refs, ref{name: s.Name, dataSum: hashSecretData(s.Data)})
	}

	sort.Slice(refs, func(i, j int) bool { return refs[i].name < refs[j].name })

	h := sha256.New()

	for _, r := range refs {
		h.Write([]byte(r.name))
		h.Write([]byte{0})
		h.Write(r.dataSum[:])
		h.Write([]byte{0})
	}

	return hex.EncodeToString(h.Sum(nil)), nil
}

// hashSecretData returns a deterministic SHA-256 of a Secret's data
// map: keys sorted, each (key, value) pair length-prefixed with a
// null separator so distinct maps cannot collide via boundary tricks.
// We hash the payload directly rather than its size or version so
// only real kubeconfig content changes (CA rotation, server URL
// change, key rotation) bump the fingerprint.
func hashSecretData(data map[string][]byte) [sha256.Size]byte {
	keys := make([]string, 0, len(data))
	for k := range data {
		keys = append(keys, k)
	}

	sort.Strings(keys)

	h := sha256.New()

	for _, k := range keys {
		h.Write([]byte(k))
		h.Write([]byte{0})
		h.Write(data[k])
		h.Write([]byte{0})
	}

	var out [sha256.Size]byte

	copy(out[:], h.Sum(nil))

	return out
}

// isTenantKubeconfigName reports whether a Secret name follows the
// `kubernetes-switchcloud-<tenant>-admin-kubeconfig` convention.
// Shared by the predicate and the fingerprint list so they agree on
// the filter.
func isTenantKubeconfigName(name string) bool {
	if !strings.HasPrefix(name, multicluster.KubeconfigNamePrefix) || !strings.HasSuffix(name, multicluster.KubeconfigSuffix) {
		return false
	}

	tenant := strings.TrimSuffix(strings.TrimPrefix(name, multicluster.KubeconfigNamePrefix), multicluster.KubeconfigSuffix)

	return tenant != ""
}

// isTenantKubeconfigSecret is the event-time predicate. Returns true
// when the Secret is in the tenant-root namespace and the name
// matches the kubeconfig convention. Applied to Create, Update,
// Delete and Generic events alike — any of those is enough to change
// the fingerprint.
func isTenantKubeconfigSecret(obj client.Object) bool {
	if obj.GetNamespace() != multicluster.TenantNamespace {
		return false
	}

	return isTenantKubeconfigName(obj.GetName())
}
