package sniproxy_test

import (
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"crypto/ed25519"
	"encoding/pem"
	"math/big"
	"net"
	"testing"
	"time"

	"github.com/aenix-org/kubernetes-switchcloud/talos-trustd-router/internal/sniproxy"
)

// makeSelfSignedCert returns a TLS certificate for the given hostname.
func makeSelfSignedCert(t *testing.T, hostname string) tls.Certificate {
	t.Helper()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: hostname},
		DNSNames:     []string{hostname},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}
	certDER, _ := x509.CreateCertificate(rand.Reader, tmpl, tmpl, pub, priv)
	keyDER, _ := x509.MarshalPKCS8PrivateKey(priv)
	cert, _ := tls.X509KeyPair(
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certDER}),
		pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: keyDER}),
	)
	return cert
}

func TestReadSNI(t *testing.T) {
	const hostname = "kubernetes-switchcloud-sc-test.dev3.infra.aenix.org"

	serverCert := makeSelfSignedCert(t, hostname)
	serverTLS := &tls.Config{Certificates: []tls.Certificate{serverCert}}

	// Use a local listener pair.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	// Server side: accept raw, extract SNI, then serve TLS.
	gotSNI := make(chan string, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			gotSNI <- "error: " + err.Error()
			return
		}
		sni, peek, err := sniproxy.ReadSNI(raw)
		if err != nil {
			gotSNI <- "error: " + err.Error()
			raw.Close()
			return
		}
		gotSNI <- sni
		// Complete the TLS handshake so the client doesn't hang.
		tlsConn := tls.Server(peek, serverTLS)
		_ = tlsConn.Handshake()
		tlsConn.Close()
	}()

	// Client side: connect with TLS and set ServerName (SNI).
	clientTLS := &tls.Config{
		ServerName:         hostname,
		InsecureSkipVerify: true, //nolint:gosec // test only
	}
	conn, err := tls.Dial("tcp", ln.Addr().String(), clientTLS)
	if err != nil && conn == nil {
		// Connection may fail after SNI is extracted; that's fine.
	}
	if conn != nil {
		conn.Close()
	}

	sni := <-gotSNI
	if sni != hostname {
		t.Errorf("expected SNI %q, got %q", hostname, sni)
	}
}

func TestReadSNI_ReplaysBytesAfterPeek(t *testing.T) {
	const hostname = "test.example.com"

	serverCert := makeSelfSignedCert(t, hostname)
	serverTLS := &tls.Config{Certificates: []tls.Certificate{serverCert}}

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()

	handshakeOK := make(chan bool, 1)
	go func() {
		raw, err := ln.Accept()
		if err != nil {
			handshakeOK <- false
			return
		}
		_, peek, err := sniproxy.ReadSNI(raw)
		if err != nil {
			raw.Close()
			handshakeOK <- false
			return
		}
		// Must complete TLS handshake using the PeekConn — proves bytes were replayed.
		tlsConn := tls.Server(peek, serverTLS)
		err = tlsConn.Handshake()
		handshakeOK <- (err == nil)
		tlsConn.Close()
	}()

	conn, err := tls.Dial("tcp", ln.Addr().String(), &tls.Config{
		ServerName:         hostname,
		InsecureSkipVerify: true, //nolint:gosec // test only
	})
	if err != nil {
		t.Logf("client dial error (may be expected): %v", err)
	}
	if conn != nil {
		conn.Close()
	}

	if ok := <-handshakeOK; !ok {
		t.Error("TLS handshake failed after SNI peek — PeekConn did not replay bytes correctly")
	}
}
