// Package snirewrite reads the TLS ClientHello at the start of a TCP stream
// and inserts (or replaces) the server_name extension so the upstream
// receives the SNI we want.
//
// The transformation only touches the SNI bytes and the length fields that
// cover it: the inner ServerNameList, the extension data, the extensions
// block, the handshake message, and the TLS record. The rest of the
// ClientHello passes through unchanged, so TLS stays end-to-end (kubelet
// keeps its client cert and the apiserver still sees it).
package snirewrite

import (
	"encoding/binary"
	"errors"
	"fmt"
)

const (
	tlsRecordHandshake      = 0x16
	handshakeClientHello    = 0x01
	extensionServerName     = 0x0000
	serverNameTypeHostName  = 0x00
	maxClientHelloRecordLen = 16384 // hard cap on the bytes we will buffer
)

// Rewrite returns a ClientHello byte slice with the server_name extension set
// to host. clientHello must be exactly one TLS record carrying a ClientHello
// (the leading 5 bytes are the TLS record header). It is the caller's
// responsibility to drain the full record from the wire before invoking
// Rewrite.
func Rewrite(clientHello []byte, host string) ([]byte, error) {
	if len(host) == 0 {
		return nil, errors.New("snirewrite: empty host")
	}
	if len(host) > 0xFFFF {
		return nil, errors.New("snirewrite: host too long")
	}

	if len(clientHello) < 5 {
		return nil, errors.New("snirewrite: short TLS record header")
	}
	if clientHello[0] != tlsRecordHandshake {
		return nil, fmt.Errorf("snirewrite: not a TLS handshake record (type=0x%02x)", clientHello[0])
	}

	recordLen := int(binary.BigEndian.Uint16(clientHello[3:5]))
	if recordLen > maxClientHelloRecordLen {
		return nil, fmt.Errorf("snirewrite: TLS record too large: %d", recordLen)
	}
	if len(clientHello) < 5+recordLen {
		return nil, fmt.Errorf("snirewrite: short TLS record: have %d, need %d", len(clientHello)-5, recordLen)
	}

	body := clientHello[5 : 5+recordLen]
	if len(body) < 4 || body[0] != handshakeClientHello {
		return nil, fmt.Errorf("snirewrite: not a ClientHello (type=0x%02x)", body[0])
	}
	hsBodyLen := int(uint(body[1])<<16 | uint(body[2])<<8 | uint(body[3]))
	if len(body) < 4+hsBodyLen {
		return nil, fmt.Errorf("snirewrite: short ClientHello body: have %d, need %d", len(body)-4, hsBodyLen)
	}
	hs := body[4 : 4+hsBodyLen]

	extStart, extLen, err := locateExtensions(hs)
	if err != nil {
		return nil, err
	}

	var extBlock []byte
	if extStart < len(hs) {
		// extStart points at the 2-byte extensions length field; the block
		// content runs for extLen bytes after that.
		extBlock = hs[extStart : extStart+2+extLen]
	}
	newExtensions, err := upsertServerName(extBlock, host)
	if err != nil {
		return nil, err
	}

	// Rebuild the message from inside out. We always allocate a fresh slice;
	// callers may keep clientHello around for logging.
	newHS := make([]byte, 0, 4+extStart+len(newExtensions))
	newHS = append(newHS, handshakeClientHello)
	newHS = append(newHS, 0, 0, 0) // length placeholder
	newHS = append(newHS, hs[:extStart]...)
	newHS = append(newHS, newExtensions...)

	hsLen := len(newHS) - 4
	if hsLen > 0xFFFFFF {
		return nil, errors.New("snirewrite: handshake too large")
	}
	newHS[1] = byte(hsLen >> 16)
	newHS[2] = byte(hsLen >> 8)
	newHS[3] = byte(hsLen)

	if len(newHS) > 0xFFFF {
		return nil, errors.New("snirewrite: TLS record too large after rewrite")
	}

	out := make([]byte, 5, 5+len(newHS))
	copy(out, clientHello[:5])
	binary.BigEndian.PutUint16(out[3:5], uint16(len(newHS)))
	out = append(out, newHS...)
	return out, nil
}

// locateExtensions parses the ClientHello body up to the extensions block and
// returns the offset of the extensions length field and the length of the
// extensions block.
func locateExtensions(hs []byte) (start int, length int, err error) {
	if len(hs) < 34 {
		return 0, 0, fmt.Errorf("snirewrite: ClientHello too short for version+random: %d", len(hs))
	}
	pos := 2 + 32 // legacy_version + random

	if pos+1 > len(hs) {
		return 0, 0, errors.New("snirewrite: truncated session_id length")
	}
	sessionIDLen := int(hs[pos])
	pos += 1 + sessionIDLen

	if pos+2 > len(hs) {
		return 0, 0, errors.New("snirewrite: truncated cipher_suites length")
	}
	cipherLen := int(binary.BigEndian.Uint16(hs[pos : pos+2]))
	pos += 2 + cipherLen

	if pos+1 > len(hs) {
		return 0, 0, errors.New("snirewrite: truncated compression_methods length")
	}
	compLen := int(hs[pos])
	pos += 1 + compLen

	if pos == len(hs) {
		// No extensions block at all (TLS 1.0 ClientHello without extensions).
		return pos, 0, nil
	}
	if pos+2 > len(hs) {
		return 0, 0, errors.New("snirewrite: truncated extensions length")
	}
	extLen := int(binary.BigEndian.Uint16(hs[pos : pos+2]))
	if pos+2+extLen != len(hs) {
		return 0, 0, fmt.Errorf("snirewrite: extensions length mismatch: declared %d, have %d", extLen, len(hs)-pos-2)
	}
	return pos, extLen, nil
}

// upsertServerName rebuilds the extensions block (including its 2-byte length
// prefix) with a server_name extension carrying host. If the extension is
// absent, it is appended.
//
// existing is positioned at the extensions-length field of the original
// ClientHello (so it may be empty if there were no extensions).
func upsertServerName(existing []byte, host string) ([]byte, error) {
	rest := []byte{}
	if len(existing) >= 2 {
		// Skip the existing length field, keep all extensions except SNI.
		rest = make([]byte, 0, len(existing)-2)
		i := 2
		for i+4 <= len(existing) {
			extType := binary.BigEndian.Uint16(existing[i : i+2])
			extDataLen := int(binary.BigEndian.Uint16(existing[i+2 : i+4]))
			if i+4+extDataLen > len(existing) {
				return nil, errors.New("snirewrite: extension overruns extensions block")
			}
			if extType != extensionServerName {
				rest = append(rest, existing[i:i+4+extDataLen]...)
			}
			i += 4 + extDataLen
		}
		if i != len(existing) {
			return nil, errors.New("snirewrite: trailing bytes in extensions block")
		}
	}

	sniExt := buildSNIExtension(host)
	body := make([]byte, 0, 2+len(sniExt)+len(rest))
	body = append(body, 0, 0) // extensions length placeholder
	body = append(body, sniExt...)
	body = append(body, rest...)

	innerLen := len(body) - 2
	if innerLen > 0xFFFF {
		return nil, errors.New("snirewrite: extensions block too large")
	}
	binary.BigEndian.PutUint16(body[0:2], uint16(innerLen))
	return body, nil
}

func buildSNIExtension(host string) []byte {
	// server_name extension layout:
	//   ExtensionType (2)         = 0x0000
	//   ExtensionDataLength (2)
	//   ServerNameListLength (2)
	//   NameType (1)              = 0 (host_name)
	//   HostName length (2)
	//   HostName bytes
	hostLen := uint16(len(host))
	nameEntryLen := 1 + 2 + int(hostLen)
	listLen := uint16(nameEntryLen)
	dataLen := uint16(2 + nameEntryLen)

	out := make([]byte, 4+2+1+2+len(host))
	binary.BigEndian.PutUint16(out[0:2], extensionServerName)
	binary.BigEndian.PutUint16(out[2:4], dataLen)
	binary.BigEndian.PutUint16(out[4:6], listLen)
	out[6] = serverNameTypeHostName
	binary.BigEndian.PutUint16(out[7:9], hostLen)
	copy(out[9:], host)
	return out
}
