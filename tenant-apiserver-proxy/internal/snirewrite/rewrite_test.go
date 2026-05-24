package snirewrite_test

import (
	"crypto/tls"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/aenix-org/kubernetes-switchcloud/tenant-apiserver-proxy/internal/snirewrite"
)

// captureClientHello starts a TLS-like server that records the first TCP
// payload it receives, then immediately closes. The handshake is expected
// to fail on the client side — we only care about the bytes on the wire.
func captureClientHello(t *testing.T, dial func(addr string) (net.Conn, error)) []byte {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = l.Close() })

	captured := make(chan []byte, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		buf := make([]byte, 4096)
		_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		n, _ := conn.Read(buf)
		captured <- buf[:n]
	}()

	c, err := dial(l.Addr().String())
	if err == nil {
		_ = c.Close()
	}

	select {
	case b := <-captured:
		return b
	case <-time.After(3 * time.Second):
		t.Fatal("did not capture ClientHello")
	}
	return nil
}

func TestRewrite_InsertsSNIWhenAbsent(t *testing.T) {
	// Go's TLS client suppresses SNI when ServerName is empty AND the URL host
	// is an IP literal. Emulate that by leaving ServerName blank.
	hello := captureClientHello(t, func(addr string) (net.Conn, error) {
		return tls.Dial("tcp", addr, &tls.Config{
			InsecureSkipVerify: true,
			// ServerName intentionally empty -> no SNI
		})
	})

	out, err := snirewrite.Rewrite(hello, "example.test")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if !strings.Contains(string(out), "example.test") {
		t.Fatalf("rewritten ClientHello does not contain new SNI value")
	}
	if got := parseSNI(t, out); got != "example.test" {
		t.Fatalf("SNI = %q, want %q", got, "example.test")
	}
}

func TestRewrite_ReplacesExistingSNI(t *testing.T) {
	hello := captureClientHello(t, func(addr string) (net.Conn, error) {
		return tls.Dial("tcp", addr, &tls.Config{
			InsecureSkipVerify: true,
			ServerName:         "original.example",
		})
	})

	out, err := snirewrite.Rewrite(hello, "rewritten.example.test")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	if got := parseSNI(t, out); got != "rewritten.example.test" {
		t.Fatalf("SNI = %q, want %q", got, "rewritten.example.test")
	}
	if strings.Contains(string(out), "original.example") {
		t.Fatalf("rewritten ClientHello still contains old SNI value")
	}
}

func TestRewrite_PreservesOtherExtensions(t *testing.T) {
	hello := captureClientHello(t, func(addr string) (net.Conn, error) {
		return tls.Dial("tcp", addr, &tls.Config{
			InsecureSkipVerify: true,
			NextProtos:         []string{"h2", "http/1.1"},
			ServerName:         "original.example",
		})
	})

	out, err := snirewrite.Rewrite(hello, "rewritten.example.test")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}
	// ALPN strings should still be in the rewritten ClientHello.
	if !strings.Contains(string(out), "h2") || !strings.Contains(string(out), "http/1.1") {
		t.Fatalf("rewritten ClientHello dropped ALPN extension")
	}
}

func TestRewrite_RoundTripsRandomBlock(t *testing.T) {
	hello := captureClientHello(t, func(addr string) (net.Conn, error) {
		return tls.Dial("tcp", addr, &tls.Config{InsecureSkipVerify: true})
	})

	// The Random field is 32 bytes starting at offset 5 (record header) + 4
	// (handshake header) + 2 (legacy_version) = 11.
	const randomOffset = 11
	orig := make([]byte, 32)
	copy(orig, hello[randomOffset:randomOffset+32])

	out, err := snirewrite.Rewrite(hello, "rewritten.example.test")
	if err != nil {
		t.Fatalf("Rewrite: %v", err)
	}

	for i, b := range orig {
		if out[randomOffset+i] != b {
			t.Fatalf("Random byte %d differs: got 0x%02x want 0x%02x", i, out[randomOffset+i], b)
		}
	}
}

// parseSNI is a tiny ClientHello SNI extractor used only by tests, to avoid
// asserting on bytes of an internal layout.
func parseSNI(t *testing.T, record []byte) string {
	t.Helper()
	if len(record) < 5 {
		t.Fatalf("record too short")
	}
	body := record[5 : 5+int(record[3])<<8|int(record[4])]
	hs := body[4 : 4+int(body[1])<<16|int(body[2])<<8|int(body[3])]
	pos := 2 + 32
	pos += 1 + int(hs[pos])
	pos += 2 + (int(hs[pos])<<8 | int(hs[pos+1]))
	pos += 1 + int(hs[pos])
	pos += 2
	end := len(hs)
	for pos+4 <= end {
		extType := int(hs[pos])<<8 | int(hs[pos+1])
		extLen := int(hs[pos+2])<<8 | int(hs[pos+3])
		pos += 4
		if extType != 0x0000 {
			pos += extLen
			continue
		}
		data := hs[pos : pos+extLen]
		// data: listLen(2) + nameType(1) + hostLen(2) + host
		hostLen := int(data[3])<<8 | int(data[4])
		return string(data[5 : 5+hostLen])
	}
	return ""
}
