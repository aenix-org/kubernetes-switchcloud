// Package ca loads Talos CA from a secrets bundle and provides signing operations.
package ca

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/yaml.v3"
)

// bundle mirrors the structure of the Talos secrets bundle YAML.
type bundle struct {
	Certs struct {
		OS struct {
			Crt string `yaml:"crt"`
			Key string `yaml:"key"`
		} `yaml:"os"`
	} `yaml:"certs"`
}

// Loader holds the parsed Talos CA certificate and key.
type Loader struct {
	caCert *x509.Certificate
	caKey  crypto.Signer
	caPEM  []byte
}

// NewLoader reads the Talos secrets bundle from bundleDir/bundle.
// bundleDir is the mount path of the kubernetes-switchcloud-<name>-talos Secret.
func NewLoader(bundleDir string) (*Loader, error) {
	data, err := os.ReadFile(filepath.Join(bundleDir, "bundle"))
	if err != nil {
		return nil, fmt.Errorf("read bundle: %w", err)
	}

	var b bundle
	if err := yaml.Unmarshal(data, &b); err != nil {
		return nil, fmt.Errorf("parse bundle: %w", err)
	}

	if b.Certs.OS.Crt == "" || b.Certs.OS.Key == "" {
		return nil, fmt.Errorf("bundle missing certs.os.crt or certs.os.key")
	}

	// Bundle values are base64-encoded PEM — decode before parsing.
	caPEMRaw, err := base64.StdEncoding.DecodeString(b.Certs.OS.Crt)
	if err != nil {
		return nil, fmt.Errorf("base64-decode CA cert: %w", err)
	}
	caPEM := caPEMRaw

	block, _ := pem.Decode(caPEM)
	if block == nil {
		return nil, fmt.Errorf("failed to decode CA PEM")
	}

	caCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA cert: %w", err)
	}

	caKeyRaw, err := base64.StdEncoding.DecodeString(b.Certs.OS.Key)
	if err != nil {
		return nil, fmt.Errorf("base64-decode CA key: %w", err)
	}

	keyBlock, _ := pem.Decode(caKeyRaw)
	if keyBlock == nil {
		return nil, fmt.Errorf("failed to decode CA key PEM")
	}

	caKey, err := parsePrivateKey(keyBlock.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CA key: %w", err)
	}

	return &Loader{caCert: caCert, caKey: caKey, caPEM: caPEM}, nil
}

// SignCSR signs a PEM-encoded CSR with the Talos CA and returns the signed cert PEM.
func (l *Loader) SignCSR(csrPEM []byte) ([]byte, error) {
	block, _ := pem.Decode(csrPEM)
	if block == nil {
		return nil, fmt.Errorf("failed to decode CSR PEM")
	}

	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("parse CSR: %w", err)
	}

	if err := csr.CheckSignature(); err != nil {
		return nil, fmt.Errorf("invalid CSR signature: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("generate serial: %w", err)
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      csr.Subject,
		DNSNames:     csr.DNSNames,
		IPAddresses:  csr.IPAddresses,
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(8760 * time.Hour), // 1 year
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, l.caCert, csr.PublicKey, l.caKey)
	if err != nil {
		return nil, fmt.Errorf("sign certificate: %w", err)
	}

	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}), nil
}

// CACertPEM returns the Talos CA certificate in PEM format.
func (l *Loader) CACertPEM() []byte {
	return l.caPEM
}

// TLSCredentials generates an ephemeral server TLS certificate signed by the Talos CA.
// Workers verify this cert against their known Talos CA — this proves we are the
// legitimate trustd for this cluster.
// clusterHostname is added to the DNS SAN so Talos workers can verify the hostname.
func (l *Loader) TLSCredentials(clusterHostname string) (tls.Certificate, *x509.CertPool, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("generate server key: %w", err)
	}

	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("generate serial: %w", err)
	}

	dnsNames := []string{}
	if clusterHostname != "" {
		dnsNames = append(dnsNames, clusterHostname)
	}

	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{Organization: []string{"talos"}},
		DNSNames:     dnsNames,
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(8760 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}

	certDER, err := x509.CreateCertificate(rand.Reader, tmpl, l.caCert, priv.Public(), l.caKey)
	if err != nil {
		return tls.Certificate{}, nil, fmt.Errorf("sign server cert: %w", err)
	}

	certPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER})
	keyBytes, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return tls.Certificate{}, nil, err
	}
	keyPEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyBytes})

	tlsCert, err := tls.X509KeyPair(certPEM, keyPEM)
	if err != nil {
		return tls.Certificate{}, nil, err
	}

	pool := x509.NewCertPool()
	pool.AddCert(l.caCert)

	return tlsCert, pool, nil
}

func parsePrivateKey(der []byte) (crypto.Signer, error) {
	if key, err := x509.ParseECPrivateKey(der); err == nil {
		return key, nil
	}
	if key, err := x509.ParsePKCS8PrivateKey(der); err == nil {
		switch k := key.(type) {
		case *ecdsa.PrivateKey:
			return k, nil
		case *rsa.PrivateKey:
			return k, nil
		case ed25519.PrivateKey:
			return k, nil
		}
	}
	if key, err := x509.ParsePKCS1PrivateKey(der); err == nil {
		return key, nil
	}
	return nil, fmt.Errorf("unsupported private key type")
}
