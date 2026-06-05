/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package controller

import (
	"context"
	"testing"
	"time"

	"github.com/go-logr/logr"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/ksc"
)

func newHRForTenant(tenant string, deleting bool) *unstructured.Unstructured {
	hr := &unstructured.Unstructured{}
	hr.SetGroupVersionKind(helmReleaseGVK)
	hr.SetNamespace(ksc.TenantNamespace)
	hr.SetName("kubernetes-switchcloud-" + tenant)
	hr.SetUID(types.UID("test-uid-" + tenant))

	if deleting {
		now := metav1.NewTime(time.Now())
		hr.SetDeletionTimestamp(&now)
		// kubectl always leaves at least one finalizer on a
		// terminating object; fake.Client refuses to materialise a
		// deletionTimestamp without one, mirroring real apiserver
		// behaviour.
		hr.SetFinalizers([]string{"loadbalancer.switchcloud.aenix.io/cluster-cleanup"})
	}

	return hr
}

func newReconcilerForTenant(t *testing.T, tenant string, objs ...client.Object) *tenantServiceReconciler {
	t.Helper()

	scheme := runtime.NewScheme()

	// We only need HelmRelease for the management client. fake.Client
	// will refuse to List the GVK without something registered, but
	// for typed-by-GVK Get against unstructured.Unstructured we just
	// need the scheme to exist.
	scheme.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: helmReleaseGVK.Group, Version: helmReleaseGVK.Version, Kind: helmReleaseGVK.Kind},
		&unstructured.Unstructured{},
	)
	scheme.AddKnownTypeWithName(
		schema.GroupVersionKind{Group: helmReleaseGVK.Group, Version: helmReleaseGVK.Version, Kind: helmReleaseGVK.Kind + "List"},
		&unstructured.UnstructuredList{},
	)

	mgmt := fake.NewClientBuilder().WithScheme(scheme).WithObjects(objs...).Build()

	return &tenantServiceReconciler{
		tenant:     tenant,
		mgmtClient: mgmt,
		log:        logr.Discard(),
	}
}

func TestIsHelmReleaseDeleting_NotFoundIsTrue(t *testing.T) {
	t.Parallel()

	// No HR in the cache at all: tenant is on its way out (HR already
	// fully deleted by Flux). The probe must return true so the
	// service controller stops trying to provision.
	r := newReconcilerForTenant(t, "mesh1")

	deleting, err := r.isHelmReleaseDeleting(context.Background())
	if err != nil {
		t.Fatalf("isHelmReleaseDeleting NotFound case: %v", err)
	}

	if !deleting {
		t.Fatal("missing HR must be treated as deleting")
	}
}

func TestIsHelmReleaseDeleting_AliveIsFalse(t *testing.T) {
	t.Parallel()

	// Healthy HR, no deletionTimestamp: tenant is operational, do not
	// short-circuit.
	r := newReconcilerForTenant(t, "mesh1", newHRForTenant("mesh1", false))

	deleting, err := r.isHelmReleaseDeleting(context.Background())
	if err != nil {
		t.Fatalf("isHelmReleaseDeleting alive case: %v", err)
	}

	if deleting {
		t.Fatal("HR without deletionTimestamp must not be reported as deleting")
	}
}

func TestIsHelmReleaseDeleting_TerminatingIsTrue(t *testing.T) {
	t.Parallel()

	// HR present with deletionTimestamp: cluster controller's sweep
	// is the source of truth; the per-Service reconciler must
	// short-circuit so it does not re-create the resources the sweep
	// is concurrently tearing down.
	r := newReconcilerForTenant(t, "mesh1", newHRForTenant("mesh1", true))

	deleting, err := r.isHelmReleaseDeleting(context.Background())
	if err != nil {
		t.Fatalf("isHelmReleaseDeleting terminating case: %v", err)
	}

	if !deleting {
		t.Fatal("HR with deletionTimestamp must be reported as deleting")
	}
}

func TestIsHelmReleaseDeleting_LooksUpByTenantName(t *testing.T) {
	t.Parallel()

	// Two tenants present, only `mesh2` is being torn down. The probe
	// for mesh1 must report alive and not be confused by mesh2's
	// deletionTimestamp. Guards against a future refactor that
	// accidentally lists instead of getting by deterministic name.
	r := newReconcilerForTenant(t, "mesh1",
		newHRForTenant("mesh1", false),
		newHRForTenant("mesh2", true),
	)

	deleting, err := r.isHelmReleaseDeleting(context.Background())
	if err != nil {
		t.Fatalf("isHelmReleaseDeleting mixed case: %v", err)
	}

	if deleting {
		t.Fatal("probe must scope to this tenant's HR, not be confused by a sibling terminating")
	}
}

// brokenClient simulates an apiserver error that is neither NotFound
// nor success. The probe must propagate the error rather than fall
// open (treating transient errors as "alive" would let create-path
// fire against a terminating tenant on a temporary apiserver blip).
type brokenClient struct {
	client.Client
}

func (b brokenClient) Get(_ context.Context, _ client.ObjectKey, _ client.Object, _ ...client.GetOption) error {
	return apierrors.NewServiceUnavailable("simulated apiserver hiccup")
}

func TestIsHelmReleaseDeleting_PropagatesNonNotFoundErrors(t *testing.T) {
	t.Parallel()

	r := &tenantServiceReconciler{
		tenant:     "mesh1",
		mgmtClient: brokenClient{},
		log:        logr.Discard(),
	}

	_, err := r.isHelmReleaseDeleting(context.Background())
	if err == nil {
		t.Fatal("transient apiserver error must be propagated, not swallowed as alive/dead")
	}
}
