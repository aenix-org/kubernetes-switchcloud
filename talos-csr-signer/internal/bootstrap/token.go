// Package bootstrap manages the Kubernetes bootstrap token required for
// Talos worker nodes to authenticate against a Kamaji-hosted control plane.
package bootstrap

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"

	corev1 "k8s.io/api/core/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"gopkg.in/yaml.v3"
)

// secretsBundle mirrors only the fields we need from the CABPT secrets bundle.
type secretsBundle struct {
	Secrets struct {
		BootstrapToken string `yaml:"bootstraptoken"`
	} `yaml:"secrets"`
}

// TokenFromBundle reads the Talos secrets bundle at bundleDir/bundle
// and returns the bootstrap token in "id.secret" format.
func TokenFromBundle(bundleDir string) (string, error) {
	data, err := os.ReadFile(filepath.Join(bundleDir, "bundle"))
	if err != nil {
		return "", fmt.Errorf("read bundle: %w", err)
	}

	var b secretsBundle
	if err := yaml.Unmarshal(data, &b); err != nil {
		return "", fmt.Errorf("parse bundle: %w", err)
	}

	if b.Secrets.BootstrapToken == "" {
		return "", fmt.Errorf("secrets.bootstraptoken not found in bundle")
	}

	return b.Secrets.BootstrapToken, nil
}

// EnsureToken creates the bootstrap-token-<id> Secret in kube-system of the
// workload cluster if it does not already exist. It is idempotent.
func EnsureToken(ctx context.Context, cs kubernetes.Interface, token string, log *slog.Logger) error {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return fmt.Errorf("invalid bootstrap token format %q: expected id.secret", token)
	}
	tokenID, tokenSecret := parts[0], parts[1]

	secretName := "bootstrap-token-" + tokenID

	_, err := cs.CoreV1().Secrets("kube-system").Get(ctx, secretName, metav1.GetOptions{})
	if err == nil {
		log.Info("bootstrap token already exists", "name", secretName)
		return nil
	}
	if !k8serrors.IsNotFound(err) {
		return fmt.Errorf("get bootstrap token: %w", err)
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      secretName,
			Namespace: "kube-system",
		},
		Type: "bootstrap.kubernetes.io/token",
		StringData: map[string]string{
			"token-id":                       tokenID,
			"token-secret":                   tokenSecret,
			"usage-bootstrap-authentication": "true",
			"usage-bootstrap-signing":        "true",
			"auth-extra-groups":              "system:bootstrappers:kubeadm:default-node-token",
		},
	}

	if _, err := cs.CoreV1().Secrets("kube-system").Create(ctx, secret, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("create bootstrap token: %w", err)
	}

	log.Info("bootstrap token created", "name", secretName)
	return nil
}
