package ca_test

import (
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aenix-org/kubernetes-switchcloud/talos-csr-signer/internal/ca"
)

// makeTestBundle creates a temporary bundle directory with a self-signed Talos CA.
func makeTestBundle(t *testing.T) (dir string, caKey ed25519.PrivateKey, caCert *x509.Certificate) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"talos"}},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		IsCA:         true,
		KeyUsage:     x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	if err != nil {
		t.Fatalf("create CA cert: %v", err)
	}

	caCert, err = x509.ParseCertificate(certDER)
	if err != nil {
		t.Fatalf("parse CA cert: %v", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})

	keyDER, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		t.Fatalf("marshal key: %v", err)
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER})

	// loader.go expects base64-encoded PEM (matching the CABPT secrets bundle format).
	bundleYAML := "certs:\n  os:\n    crt: " + base64.StdEncoding.EncodeToString(certPEM) + "\n" +
		"    key: " + base64.StdEncoding.EncodeToString(keyPEM) + "\n"

	dir = t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "bundle"), []byte(bundleYAML), 0o600); err != nil {
		t.Fatalf("write bundle: %v", err)
	}

	return dir, priv, caCert
}

func TestLoader_LoadsCA(t *testing.T) {
	dir, _, _ := makeTestBundle(t)

	loader, err := ca.NewLoader(dir)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	if len(loader.CACertPEM()) == 0 {
		t.Fatal("expected non-empty CA PEM")
	}
}

func TestLoader_TLSCredentials(t *testing.T) {
	dir, _, caCert := makeTestBundle(t)

	loader, err := ca.NewLoader(dir)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	tlsCert, pool, err := loader.TLSCredentials("")
	if err != nil {
		t.Fatalf("TLSCredentials: %v", err)
	}

	if len(tlsCert.Certificate) == 0 {
		t.Fatal("expected non-empty TLS cert chain")
	}

	// Verify that the ephemeral cert is signed by our CA.
	leaf, err := x509.ParseCertificate(tlsCert.Certificate[0])
	if err != nil {
		t.Fatalf("parse leaf cert: %v", err)
	}
	if _, err := leaf.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
		t.Fatalf("ephemeral cert not signed by CA: %v", err)
	}

	_ = caCert
}

func TestLoader_SignCSR(t *testing.T) {
	dir, _, caCert := makeTestBundle(t)

	loader, err := ca.NewLoader(dir)
	if err != nil {
		t.Fatalf("NewLoader: %v", err)
	}

	// Generate a CSR.
	_, reqKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	csrTmpl := &x509.CertificateRequest{
		Subject:     pkix.Name{CommonName: "apid"},
		IPAddresses: []net.IP{net.ParseIP("10.0.1.100")},
		DNSNames:    []string{"test-cluster.example.com"},
	}
	csrDER, err := x509.CreateCertificateRequest(rand.Reader, csrTmpl, reqKey)
	if err != nil {
		t.Fatalf("create CSR: %v", err)
	}
	csrPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csrDER})

	signedPEM, err := loader.SignCSR(csrPEM)
	if err != nil {
		t.Fatalf("SignCSR: %v", err)
	}

	block, _ := pem.Decode(signedPEM)
	if block == nil {
		t.Fatal("expected PEM output from SignCSR")
	}
	signed, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse signed cert: %v", err)
	}

	pool := x509.NewCertPool()
	pool.AddCert(caCert)
	if _, err := signed.Verify(x509.VerifyOptions{Roots: pool}); err != nil {
		t.Fatalf("signed cert not verified by CA: %v", err)
	}

	if signed.Subject.CommonName != "apid" {
		t.Errorf("expected CN=apid, got %s", signed.Subject.CommonName)
	}
}
