// Command tenant-apiserver-proxy listens on a local TCP port and forwards
// every connection to an upstream Kubernetes apiserver, rewriting the TLS
// ClientHello's server_name extension on the way out so the upstream
// nginx-ingress (SSL passthrough) selects the right backend.
//
// The proxy never terminates TLS — it modifies only the bytes of the SNI
// extension on the wire — so kubelet's client certificate continues all the
// way to the apiserver, preserving mTLS authentication.
package main

import (
	"context"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aenix-org/kubernetes-switchcloud/tenant-apiserver-proxy/internal/snirewrite"
)

const (
	tlsRecordHeaderLen = 5
	tlsRecordHandshake = 0x16
	maxClientHelloLen  = 16384
	dialTimeout        = 10 * time.Second
	helloReadTimeout   = 10 * time.Second
)

func main() {
	listen := flag.String("listen", ":6443", "TCP listen address")
	upstream := flag.String("upstream", "", "upstream apiserver host:port (host is also used as the rewritten SNI)")
	sni := flag.String("sni", "", "override SNI value (defaults to the host part of -upstream)")
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
	sniValue := *sni
	if sniValue == "" {
		sniValue = upstreamHost
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	lis, err := net.Listen("tcp", *listen)
	if err != nil {
		log.Error("listen failed", "addr", *listen, "err", err)
		os.Exit(1)
	}
	log.Info("proxy starting", "listen", *listen, "upstream", *upstream, "sni", sniValue)

	go func() {
		<-ctx.Done()
		lis.Close()
	}()

	for {
		conn, err := lis.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			log.Warn("accept error", "err", err)
			continue
		}
		go handle(conn, *upstream, sniValue, log)
	}
}

func handle(client net.Conn, upstream, sni string, log *slog.Logger) {
	defer client.Close()

	if err := client.SetReadDeadline(time.Now().Add(helloReadTimeout)); err != nil {
		log.Warn("set deadline failed", "peer", client.RemoteAddr(), "err", err)
		return
	}

	record, err := readClientHelloRecord(client)
	if err != nil {
		log.Warn("read ClientHello failed", "peer", client.RemoteAddr(), "err", err)
		return
	}
	// Clear the read deadline so the proxied stream is not bounded by it.
	if err := client.SetReadDeadline(time.Time{}); err != nil {
		log.Warn("clear deadline failed", "peer", client.RemoteAddr(), "err", err)
		return
	}

	rewritten, err := snirewrite.Rewrite(record, sni)
	if err != nil {
		log.Warn("rewrite ClientHello failed", "peer", client.RemoteAddr(), "err", err)
		return
	}

	up, err := net.DialTimeout("tcp", upstream, dialTimeout)
	if err != nil {
		log.Error("dial upstream failed", "upstream", upstream, "err", err)
		return
	}
	defer up.Close()

	if _, err := up.Write(rewritten); err != nil {
		log.Warn("write rewritten ClientHello upstream failed", "err", err)
		return
	}

	log.Info("proxying connection", "peer", client.RemoteAddr(), "upstream", upstream, "sni", sni)

	done := make(chan struct{}, 2)
	go func() { _, _ = io.Copy(up, client); done <- struct{}{} }()
	go func() { _, _ = io.Copy(client, up); done <- struct{}{} }()
	<-done
}

// readClientHelloRecord drains exactly one TLS handshake record from conn.
// Returns the raw bytes (5-byte header + payload).
func readClientHelloRecord(conn net.Conn) ([]byte, error) {
	header := make([]byte, tlsRecordHeaderLen)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, fmt.Errorf("read TLS record header: %w", err)
	}
	if header[0] != tlsRecordHandshake {
		return nil, fmt.Errorf("first record is not a handshake (type=0x%02x)", header[0])
	}
	recordLen := int(binary.BigEndian.Uint16(header[3:5]))
	if recordLen <= 0 || recordLen > maxClientHelloLen {
		return nil, fmt.Errorf("invalid TLS record length: %d", recordLen)
	}
	payload := make([]byte, recordLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return nil, fmt.Errorf("read TLS record payload: %w", err)
	}
	return append(header, payload...), nil
}

