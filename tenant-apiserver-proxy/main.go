// Command tenant-apiserver-proxy listens on a local TLS port and forwards
// every connection to an upstream Kubernetes apiserver, doing the second
// TLS handshake with the correct SNI so the upstream nginx-ingress (SSL
// passthrough) routes to the right backend.
//
// The proxy terminates TLS on both sides:
//
//  pod -> 10.95.0.1:443 (DNAT -> 127.0.0.1:7445)
//      -> proxy presents its own cert (cluster-CA signed, IP:10.95.0.1 SAN)
//      -> proxy decrypts, opens a NEW TLS connection to <upstream-host>:443
//         with ServerName=<upstream-host> so SNI matches the ingress
//      -> proxy verifies upstream cert against the cluster CA
//      -> HTTP bytes are piped between the two TLS sessions
//
// Both TLS sessions negotiate h2/http1.1 so HTTP/2 streams pass through
// transparently after the handshake.
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const (
	dialTimeout      = 10 * time.Second
	handshakeTimeout = 15 * time.Second
)

func main() {
	listen := flag.String("listen", "127.0.0.1:7445", "TLS listen address")
	upstream := flag.String("upstream", "", "upstream apiserver host:port (host is used as TLS ServerName when dialling upstream)")
	certFile := flag.String("cert", "/etc/tenant-apiserver-proxy/tls.crt", "path to the TLS cert presented to clients (signed by the cluster CA, SAN must include the workload kubernetes Service ClusterIP)")
	keyFile := flag.String("key", "/etc/tenant-apiserver-proxy/tls.key", "path to the TLS key for -cert")
	caFile := flag.String("ca", "/etc/tenant-apiserver-proxy/ca.crt", "CA bundle used to verify the upstream apiserver cert")
	flag.Parse()

	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))

	if *upstream == "" {
		log.Error("-upstream is required")
		os.Exit(2)
	}
	upstreamHost, _, err := net.SplitHostPort(*upstream)
	if err != nil {
		log.Error("invalid -upstream", "err", err)
		os.Exit(2)
	}

	serverCert, err := tls.LoadX509KeyPair(*certFile, *keyFile)
	if err != nil {
		log.Error("load cert/key", "err", err)
		os.Exit(1)
	}

	caPEM, err := os.ReadFile(*caFile)
	if err != nil {
		log.Error("load CA", "err", err)
		os.Exit(1)
	}
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		log.Error("CA file did not contain any PEM-encoded certificates", "path", *caFile)
		os.Exit(1)
	}

	serverCfg := &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		MinVersion:   tls.VersionTLS12,
		NextProtos:   []string{"h2", "http/1.1"},
	}
	upstreamCfg := &tls.Config{
		ServerName: upstreamHost,
		RootCAs:    caPool,
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"h2", "http/1.1"},
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	lis, err := tls.Listen("tcp", *listen, serverCfg)
	if err != nil {
		log.Error("listen failed", "addr", *listen, "err", err)
		os.Exit(1)
	}
	log.Info("proxy starting", "listen", *listen, "upstream", *upstream, "sni", upstreamHost)

	go func() {
		<-ctx.Done()
		_ = lis.Close()
	}()

	for {
		conn, err := lis.Accept()
		if err != nil {
			if errors.Is(err, net.ErrClosed) || ctx.Err() != nil {
				return
			}
			log.Warn("accept", "err", err)
			continue
		}
		go handle(conn.(*tls.Conn), *upstream, upstreamCfg, log)
	}
}

func handle(client *tls.Conn, upstream string, upstreamCfg *tls.Config, log *slog.Logger) {
	defer client.Close()

	peer := client.RemoteAddr()

	if err := client.SetDeadline(time.Now().Add(handshakeTimeout)); err != nil {
		log.Warn("set deadline", "peer", peer, "err", err)
		return
	}
	if err := client.Handshake(); err != nil {
		// Most of these are TCP probes from kubelet's readiness check —
		// they hit the TLS listener, send no bytes, and time out. Log
		// at debug-ish level by warn-only; the noise is acceptable.
		log.Warn("client handshake", "peer", peer, "err", err)
		return
	}
	_ = client.SetDeadline(time.Time{})

	dialer := &net.Dialer{Timeout: dialTimeout}
	up, err := tls.DialWithDialer(dialer, "tcp", upstream, upstreamCfg)
	if err != nil {
		log.Error("dial upstream", "upstream", upstream, "err", err)
		return
	}
	defer up.Close()

	log.Info("proxying", "peer", peer, "upstream", upstream, "client_alpn", client.ConnectionState().NegotiatedProtocol, "upstream_alpn", up.ConnectionState().NegotiatedProtocol)

	// Pipe bytes in both directions and wait for BOTH copies to finish.
	// When one direction returns (EOF or error), CloseWrite the opposite
	// peer so it observes a graceful close_notify instead of an RST. This
	// preserves long-lived bidirectional streams (HTTP/2 streams from
	// metrics-server aggregation responses, kubelet status streaming,
	// kubectl exec WebSocket) where one side may pause writes for tens of
	// seconds while the other continues sending.
	done := make(chan struct{}, 2)
	go func() {
		_, _ = io.Copy(up, client)
		_ = up.CloseWrite()
		done <- struct{}{}
	}()
	go func() {
		_, _ = io.Copy(client, up)
		_ = client.CloseWrite()
		done <- struct{}{}
	}()
	<-done
	<-done
}
