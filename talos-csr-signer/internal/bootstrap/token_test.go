package bootstrap_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"

	"github.com/aenix-org/kubernetes-switchcloud/talos-csr-signer/internal/bootstrap"
)

func TestTokenFromBundle(t *testing.T) {
	dir := t.TempDir()
	bundleYAML := `secrets:
  bootstraptoken: abc123.secretvalue1234
certs:
  os:
    crt: ""
    key: ""
`
	if err := os.WriteFile(filepath.Join(dir, "bundle"), []byte(bundleYAML), 0o600); err != nil {
		t.Fatal(err)
	}

	token, err := bootstrap.TokenFromBundle(dir)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if token != "abc123.secretvalue1234" {
		t.Errorf("got %q, want %q", token, "abc123.secretvalue1234")
	}
}

func TestTokenFromBundle_Missing(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bundle"), []byte("secrets: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	_, err := bootstrap.TokenFromBundle(dir)
	if err == nil {
		t.Fatal("expected error for missing bootstraptoken, got nil")
	}
}

func TestEnsureToken_Creates(t *testing.T) {
	cs := fake.NewClientset()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if err := bootstrap.EnsureToken(context.Background(), cs, "1iur5g.v8zzcirafl9qfdq0", log); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	secret, err := cs.CoreV1().Secrets("kube-system").Get(context.Background(), "bootstrap-token-1iur5g", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("secret not found: %v", err)
	}
	if secret.Type != corev1.SecretType("bootstrap.kubernetes.io/token") {
		t.Errorf("wrong type: %q", secret.Type)
	}
	if secret.StringData["token-id"] != "1iur5g" {
		t.Errorf("wrong token-id: %q", secret.StringData["token-id"])
	}
	if secret.StringData["token-secret"] != "v8zzcirafl9qfdq0" {
		t.Errorf("wrong token-secret: %q", secret.StringData["token-secret"])
	}
}

func TestEnsureToken_Idempotent(t *testing.T) {
	cs := fake.NewSimpleClientset(&corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "bootstrap-token-1iur5g",
			Namespace: "kube-system",
		},
		Type: "bootstrap.kubernetes.io/token",
	})
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	// Should not error if secret already exists.
	if err := bootstrap.EnsureToken(context.Background(), cs, "1iur5g.v8zzcirafl9qfdq0", log); err != nil {
		t.Fatalf("unexpected error on second call: %v", err)
	}
}

func TestEnsureToken_InvalidFormat(t *testing.T) {
	cs := fake.NewClientset()
	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	if err := bootstrap.EnsureToken(context.Background(), cs, "nodotshere", log); err == nil {
		t.Fatal("expected error for malformed token, got nil")
	}
}
