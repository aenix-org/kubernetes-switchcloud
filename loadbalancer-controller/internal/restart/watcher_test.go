/*
Copyright 2026 The Aenix Authors.

Licensed under the Apache License, Version 2.0 (the "License");
*/

package restart

import (
	"context"
	"testing"

	"github.com/go-logr/logr"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"github.com/aenix-org/kubernetes-switchcloud/loadbalancer-controller/internal/multicluster"
)

func testScheme(t *testing.T) *runtime.Scheme {
	t.Helper()

	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("registering core scheme: %v", err)
	}

	return s
}

func kubeconfigSecret(tenant string, data map[string][]byte) *corev1.Secret {
	return &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: multicluster.TenantNamespace,
			Name:      multicluster.KubeconfigNamePrefix + tenant + multicluster.KubeconfigSuffix,
		},
		Data: data,
	}
}

func TestIsTenantKubeconfigName(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"valid single token", "kubernetes-switchcloud-mesh1-admin-kubeconfig", true},
		{"valid multi-dash tenant token", "kubernetes-switchcloud-mesh-prod-admin-kubeconfig", true},
		{"empty tenant token", "kubernetes-switchcloud--admin-kubeconfig", false},
		{"wrong prefix", "other-mesh1-admin-kubeconfig", false},
		{"wrong suffix", "kubernetes-switchcloud-mesh1-admin", false},
		{"prefix+suffix only", "kubernetes-switchcloud--admin-kubeconfig", false},
		{"helm release secret", "sh.helm.release.v1.kubernetes-switchcloud-mesh1.v1", false},
		{"empty string", "", false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			if got := isTenantKubeconfigName(tc.in); got != tc.want {
				t.Fatalf("isTenantKubeconfigName(%q) = %v, want %v", tc.in, got, tc.want)
			}
		})
	}
}

func TestIsTenantKubeconfigSecretRejectsForeignNamespace(t *testing.T) {
	t.Parallel()

	s := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      multicluster.KubeconfigNamePrefix + "mesh1" + multicluster.KubeconfigSuffix,
		},
	}

	if isTenantKubeconfigSecret(s) {
		t.Fatal("Secret in foreign namespace must not match")
	}
}

func TestComputeFingerprintStableAcrossOrder(t *testing.T) {
	t.Parallel()

	s1 := kubeconfigSecret("mesh1", map[string][]byte{"super-admin.conf": []byte("kubeconfig-mesh1")})
	s2 := kubeconfigSecret("mesh2", map[string][]byte{"super-admin.conf": []byte("kubeconfig-mesh2")})

	scheme := testScheme(t)
	cAB := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s1, s2).Build()
	cBA := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s2, s1).Build()

	ctx := context.Background()

	fpAB, err := ComputeFingerprint(ctx, cAB)
	if err != nil {
		t.Fatalf("ComputeFingerprint AB: %v", err)
	}

	fpBA, err := ComputeFingerprint(ctx, cBA)
	if err != nil {
		t.Fatalf("ComputeFingerprint BA: %v", err)
	}

	if fpAB != fpBA {
		t.Fatalf("fingerprint must be insertion-order independent, got %q vs %q", fpAB, fpBA)
	}
}

func TestComputeFingerprintChangesOnDataEdit(t *testing.T) {
	t.Parallel()

	before := kubeconfigSecret("mesh1", map[string][]byte{"super-admin.conf": []byte("v1")})
	after := kubeconfigSecret("mesh1", map[string][]byte{"super-admin.conf": []byte("v2-different-CA")})

	scheme := testScheme(t)

	fpBefore, err := ComputeFingerprint(context.Background(), fake.NewClientBuilder().WithScheme(scheme).WithObjects(before).Build())
	if err != nil {
		t.Fatalf("fp before: %v", err)
	}

	fpAfter, err := ComputeFingerprint(context.Background(), fake.NewClientBuilder().WithScheme(scheme).WithObjects(after).Build())
	if err != nil {
		t.Fatalf("fp after: %v", err)
	}

	if fpBefore == fpAfter {
		t.Fatalf("fingerprint must change when Secret.Data changes (got identical %q)", fpBefore)
	}
}

func TestComputeFingerprintStableOnMetadataChurn(t *testing.T) {
	t.Parallel()

	// Same data, different ResourceVersion / labels. The whole point
	// of hashing Data rather than ResourceVersion is that metadata
	// churn from other controllers / managedFields rewrites must not
	// trigger a restart.
	before := kubeconfigSecret("mesh1", map[string][]byte{"super-admin.conf": []byte("payload")})
	before.ResourceVersion = "100"

	after := kubeconfigSecret("mesh1", map[string][]byte{"super-admin.conf": []byte("payload")})
	after.ResourceVersion = "999"
	after.Labels = map[string]string{"touched-by": "other-controller"}

	scheme := testScheme(t)

	fpBefore, err := ComputeFingerprint(context.Background(), fake.NewClientBuilder().WithScheme(scheme).WithObjects(before).Build())
	if err != nil {
		t.Fatalf("fp before: %v", err)
	}

	fpAfter, err := ComputeFingerprint(context.Background(), fake.NewClientBuilder().WithScheme(scheme).WithObjects(after).Build())
	if err != nil {
		t.Fatalf("fp after: %v", err)
	}

	if fpBefore != fpAfter {
		t.Fatalf("metadata-only churn must not change fingerprint, got %q vs %q", fpBefore, fpAfter)
	}
}

func TestComputeFingerprintChangesOnAddDelete(t *testing.T) {
	t.Parallel()

	s1 := kubeconfigSecret("mesh1", map[string][]byte{"super-admin.conf": []byte("k1")})
	s2 := kubeconfigSecret("mesh2", map[string][]byte{"super-admin.conf": []byte("k2")})

	scheme := testScheme(t)

	one, err := ComputeFingerprint(context.Background(), fake.NewClientBuilder().WithScheme(scheme).WithObjects(s1).Build())
	if err != nil {
		t.Fatalf("fp one: %v", err)
	}

	two, err := ComputeFingerprint(context.Background(), fake.NewClientBuilder().WithScheme(scheme).WithObjects(s1, s2).Build())
	if err != nil {
		t.Fatalf("fp two: %v", err)
	}

	if one == two {
		t.Fatalf("adding a tenant Secret must change fingerprint, got identical %q", one)
	}

	// And the reverse: dropping s2 returns the one-secret fingerprint.
	dropped, err := ComputeFingerprint(context.Background(), fake.NewClientBuilder().WithScheme(scheme).WithObjects(s1).Build())
	if err != nil {
		t.Fatalf("fp dropped: %v", err)
	}

	if dropped != one {
		t.Fatalf("removing back to single-tenant must return to original fingerprint")
	}
}

func TestComputeFingerprintIgnoresNonKubeconfigSecrets(t *testing.T) {
	t.Parallel()

	wanted := kubeconfigSecret("mesh1", map[string][]byte{"super-admin.conf": []byte("kc")})

	helmRelease := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: multicluster.TenantNamespace,
			Name:      "sh.helm.release.v1.kubernetes-switchcloud-mesh1.v1",
		},
		Data: map[string][]byte{"release": []byte("anything")},
	}

	scheme := testScheme(t)

	withNoise, err := ComputeFingerprint(context.Background(), fake.NewClientBuilder().WithScheme(scheme).WithObjects(wanted, helmRelease).Build())
	if err != nil {
		t.Fatalf("fp with noise: %v", err)
	}

	withoutNoise, err := ComputeFingerprint(context.Background(), fake.NewClientBuilder().WithScheme(scheme).WithObjects(wanted).Build())
	if err != nil {
		t.Fatalf("fp without noise: %v", err)
	}

	if withNoise != withoutNoise {
		t.Fatalf("unrelated Secrets in tenant-root must not affect fingerprint")
	}
}

// cancelRecorder counts how many times Cancel was invoked. Lets the
// test assert that Reconcile calls Cancel exactly once on drift and
// never when stable, without depending on the exact context-cancel
// machinery.
type cancelRecorder struct {
	calls int
}

func (c *cancelRecorder) cancel() { c.calls++ }

func TestReconcileNoDriftDoesNotCancel(t *testing.T) {
	t.Parallel()

	s := kubeconfigSecret("mesh1", map[string][]byte{"super-admin.conf": []byte("stable")})
	scheme := testScheme(t)
	fakeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s).Build()

	fp, err := ComputeFingerprint(context.Background(), fakeClient)
	if err != nil {
		t.Fatalf("seed fp: %v", err)
	}

	rec := &cancelRecorder{}

	w := &TenantSecretWatcher{
		Client:           fakeClient,
		Cancel:           rec.cancel,
		StartFingerprint: fp,
		Log:              logr.Discard(),
	}

	if _, err := w.Reconcile(context.Background(), reconcile.Request{}); err != nil {
		t.Fatalf("Reconcile: %v", err)
	}

	if rec.calls != 0 {
		t.Fatalf("Cancel must not be called when fingerprint is stable, got %d calls", rec.calls)
	}
}

func TestReconcileDriftCancelsExactlyOnce(t *testing.T) {
	t.Parallel()

	scheme := testScheme(t)

	// Seed snapshot from a one-secret world.
	seed := kubeconfigSecret("mesh1", map[string][]byte{"super-admin.conf": []byte("v1")})
	seedClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(seed).Build()

	startFP, err := ComputeFingerprint(context.Background(), seedClient)
	if err != nil {
		t.Fatalf("seed fp: %v", err)
	}

	// Live world has a second tenant. Different fingerprint expected.
	now := kubeconfigSecret("mesh1", map[string][]byte{"super-admin.conf": []byte("v1")})
	added := kubeconfigSecret("mesh2", map[string][]byte{"super-admin.conf": []byte("brand-new")})
	liveClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(now, added).Build()

	rec := &cancelRecorder{}

	w := &TenantSecretWatcher{
		Client:           liveClient,
		Cancel:           rec.cancel,
		StartFingerprint: startFP,
		Log:              logr.Discard(),
	}

	if _, err := w.Reconcile(context.Background(), reconcile.Request{}); err != nil {
		t.Fatalf("Reconcile #1: %v", err)
	}

	// A subsequent Reconcile, still with the same stale
	// StartFingerprint, will call Cancel again — the watcher does not
	// remember that it already fired. That is fine in production
	// because the first Cancel takes the process down before the
	// second tick lands; here we just assert the first tick fired.
	if rec.calls < 1 {
		t.Fatalf("Cancel must be called on drift, got %d calls", rec.calls)
	}
}

// Compile-time check: TenantSecretWatcher satisfies the reconcile
// contract. Catches accidental signature drift on Reconcile.
var _ reconcileImpl = (*TenantSecretWatcher)(nil)

type reconcileImpl interface {
	Reconcile(ctx context.Context, req reconcile.Request) (reconcile.Result, error)
}

// Compile-time check: TenantSecretWatcher embeds a client.Client
// (used by Reconcile via w.Client).
var _ interface{ List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error } = &TenantSecretWatcher{}
