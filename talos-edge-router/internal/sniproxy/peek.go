// Package sniproxy provides TLS SNI extraction without terminating the TLS connection.
package sniproxy

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

const (
	tlsRecordTypeHandshake = 0x16
	tlsHandshakeClientHello = 0x01
	tlsExtensionSNI         = 0x0000
	tlsSNITypeHostname      = 0x00

	// maxClientHelloSize is the maximum number of bytes we peek to find SNI.
	maxClientHelloSize = 4096
)

// closeWriter is satisfied by *net.TCPConn (and *net.UnixConn) and lets the
// proxy half-close the write side of a stream without tearing down the
// read side. Required so that long-lived bidirectional streams (gRPC,
// HTTP/2) don't get an RST on the still-active direction when one side
// stops writing.
type closeWriter interface {
	CloseWrite() error
}

// PeekConn wraps a net.Conn and prepends already-read bytes back to the stream.
type PeekConn struct {
	net.Conn
	buf []byte
	pos int
}

func (c *PeekConn) Read(b []byte) (int, error) {
	if c.pos < len(c.buf) {
		n := copy(b, c.buf[c.pos:])
		c.pos += n
		return n, nil
	}
	return c.Conn.Read(b)
}

// CloseWrite delegates to the wrapped connection so callers can use a
// half-close to signal end-of-data without forcing a full RST.
func (c *PeekConn) CloseWrite() error {
	if cw, ok := c.Conn.(closeWriter); ok {
		return cw.CloseWrite()
	}
	return c.Conn.Close()
}

// ReadSNI reads the TLS ClientHello from conn, extracts the SNI hostname,
// and returns a PeekConn that replays the read bytes so the downstream
// handler sees a complete unmodified stream.
func ReadSNI(conn net.Conn) (string, net.Conn, error) {
	// Read the 5-byte TLS record header first to learn the payload length.
	header := make([]byte, 5)
	if _, err := io.ReadFull(conn, header); err != nil {
		return "", nil, fmt.Errorf("read TLS record header: %w", err)
	}

	recordLen := int(binary.BigEndian.Uint16(header[3:5]))
	if recordLen > maxClientHelloSize {
		return "", nil, fmt.Errorf("TLS record too large: %d bytes", recordLen)
	}

	// Read the full payload so we always have a complete ClientHello,
	// even when it arrives in multiple TCP segments.
	payload := make([]byte, recordLen)
	if _, err := io.ReadFull(conn, payload); err != nil {
		return "", nil, fmt.Errorf("read TLS record payload: %w", err)
	}

	buf := append(header, payload...)
	sni, err := extractSNI(buf)
	if err != nil {
		return "", nil, err
	}

	return sni, &PeekConn{Conn: conn, buf: buf}, nil
}

// extractSNI parses a raw TLS ClientHello byte slice and returns the SNI hostname.
func extractSNI(data []byte) (string, error) {
	if len(data) < 5 {
		return "", fmt.Errorf("too short for TLS record header")
	}
	if data[0] != tlsRecordTypeHandshake {
		return "", fmt.Errorf("not a TLS handshake record (type=0x%02x)", data[0])
	}

	// TLS record: [type(1) version(2) length(2) payload...]
	recordLen := int(binary.BigEndian.Uint16(data[3:5]))
	if len(data) < 5+recordLen {
		return "", fmt.Errorf("incomplete TLS record: have %d, need %d", len(data)-5, recordLen)
	}
	payload := data[5 : 5+recordLen]

	// Handshake: [type(1) length(3) body...]
	if len(payload) < 4 {
		return "", fmt.Errorf("too short for handshake header")
	}
	if payload[0] != tlsHandshakeClientHello {
		return "", fmt.Errorf("not a ClientHello (type=0x%02x)", payload[0])
	}

	body := payload[4:]

	// ClientHello body: [clientVersion(2) random(32) sessionIDLen(1) sessionID(n)
	//                    cipherSuitesLen(2) cipherSuites(n) compressionLen(1) compression(n)
	//                    extensionsLen(2) extensions...]
	if len(body) < 35 {
		return "", fmt.Errorf("ClientHello body too short")
	}
	pos := 2 + 32 // skip clientVersion + random

	// skip sessionID
	sessionIDLen := int(body[pos])
	pos += 1 + sessionIDLen

	if pos+2 > len(body) {
		return "", fmt.Errorf("truncated at cipher suites")
	}
	cipherSuitesLen := int(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2 + cipherSuitesLen

	if pos+1 > len(body) {
		return "", fmt.Errorf("truncated at compression")
	}
	compressionLen := int(body[pos])
	pos += 1 + compressionLen

	// extensions
	if pos+2 > len(body) {
		return "", fmt.Errorf("no extensions present")
	}
	extLen := int(binary.BigEndian.Uint16(body[pos : pos+2]))
	pos += 2
	extEnd := pos + extLen

	if extEnd > len(body) {
		return "", fmt.Errorf("extensions length exceeds body")
	}

	for pos+4 <= extEnd {
		extType := binary.BigEndian.Uint16(body[pos : pos+2])
		extDataLen := int(binary.BigEndian.Uint16(body[pos+2 : pos+4]))
		pos += 4

		if pos+extDataLen > extEnd {
			break
		}

		if extType == tlsExtensionSNI {
			return parseSNIExtension(body[pos : pos+extDataLen])
		}
		pos += extDataLen
	}

	return "", fmt.Errorf("SNI extension not found in ClientHello")
}

// parseSNIExtension parses the SNI extension value and returns the hostname.
func parseSNIExtension(data []byte) (string, error) {
	// SNI extension: [listLen(2) [nameType(1) nameLen(2) name(n)]...]
	if len(data) < 2 {
		return "", fmt.Errorf("SNI extension too short")
	}
	listLen := int(binary.BigEndian.Uint16(data[0:2]))
	if len(data) < 2+listLen {
		return "", fmt.Errorf("SNI list truncated")
	}
	pos := 2
	end := 2 + listLen
	for pos+3 <= end {
		nameType := data[pos]
		nameLen := int(binary.BigEndian.Uint16(data[pos+1 : pos+3]))
		pos += 3
		if pos+nameLen > end {
			break
		}
		if nameType == tlsSNITypeHostname {
			return string(data[pos : pos+nameLen]), nil
		}
		pos += nameLen
	}
	return "", fmt.Errorf("no hostname entry in SNI extension")
}
