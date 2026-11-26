package datasportsapi

// A small RFC 6455 WebSocket client on the standard library: the opening handshake
// (Sec-WebSocket-Accept checked), masked client frames, fragmented messages,
// ping/pong, the closing handshake and a cap on frame and message size.

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha1"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

const (
	wsGUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

	opContinuation = 0x0
	opText         = 0x1
	opBinary       = 0x2
	opClose        = 0x8
	opPing         = 0x9
	opPong         = 0xA

	// DefaultMaxFrame is the largest frame or message the stream accepts (16 MiB);
	// a bigger one closes the socket.
	DefaultMaxFrame = 16 << 20

	closeNormal        = 1000
	closeProtocolError = 1002
	closeTooBig        = 1009
	closeNoStatus      = 1005
	closeAbnormal      = 1006
)

// CloseError is how a WebSocket connection ended: the code and reason of the close
// frame the server sent (1006 when the connection dropped without one).
type CloseError struct {
	Code   int
	Reason string
}

func (e *CloseError) Error() string {
	if e.Reason == "" {
		return fmt.Sprintf("datasportsapi: websocket closed (%d)", e.Code)
	}
	return fmt.Sprintf("datasportsapi: websocket closed (%d %s)", e.Code, e.Reason)
}

var errFrameTooBig = errors.New("datasportsapi: websocket frame larger than the limit")

// wsProtoError is a protocol violation found locally; the connection is closed with code.
type wsProtoError struct {
	code   int
	reason string
}

func (e *wsProtoError) Error() string {
	return fmt.Sprintf("datasportsapi: websocket protocol error (%d %s)", e.code, e.reason)
}

// wsConn is one WebSocket connection. Reads happen on one goroutine; writes are
// serialised by wmu and may come from any goroutine.
type wsConn struct {
	conn     net.Conn
	br       *bufio.Reader
	wmu      sync.Mutex
	client   bool // client frames are masked, server frames are not
	maxFrame int64

	closeSent bool // guarded by wmu
}

func acceptKey(key string) string {
	h := sha1.Sum([]byte(key + wsGUID))
	return base64.StdEncoding.EncodeToString(h[:])
}

// dialWS opens a WebSocket connection to rawURL (ws:// or wss://).
func dialWS(ctx context.Context, rawURL string, tlsConf *tls.Config, maxFrame int64, timeout time.Duration) (*wsConn, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("datasportsapi: stream URL: %w", err)
	}
	port := u.Port()
	switch u.Scheme {
	case "wss":
		if port == "" {
			port = "443"
		}
	case "ws":
		if port == "" {
			port = "80"
		}
	default:
		return nil, fmt.Errorf("datasportsapi: stream URL scheme %q is not ws or wss", u.Scheme)
	}
	addr := net.JoinHostPort(u.Hostname(), port)
	d := &net.Dialer{Timeout: timeout}
	raw, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("datasportsapi: stream: %w", err)
	}
	conn := raw
	deadline := time.Now().Add(timeout)
	if dl, ok := ctx.Deadline(); ok && dl.Before(deadline) {
		deadline = dl
	}
	_ = conn.SetDeadline(deadline)
	// Stop the handshake when ctx ends.
	stop := context.AfterFunc(ctx, func() { _ = raw.SetDeadline(time.Unix(1, 0)) })
	defer stop()

	if u.Scheme == "wss" {
		cfg := &tls.Config{}
		if tlsConf != nil {
			cfg = tlsConf.Clone()
		}
		if cfg.ServerName == "" {
			cfg.ServerName = u.Hostname()
		}
		cfg.NextProtos = nil // HTTP/1.1 only: an upgrade is not possible over h2
		tc := tls.Client(raw, cfg)
		if err := tc.HandshakeContext(ctx); err != nil {
			raw.Close()
			return nil, fmt.Errorf("datasportsapi: stream TLS: %w", err)
		}
		conn = tc
	}

	var nonce [16]byte
	if _, err := rand.Read(nonce[:]); err != nil {
		conn.Close()
		return nil, err
	}
	key := base64.StdEncoding.EncodeToString(nonce[:])
	req := &http.Request{
		Method:     http.MethodGet,
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     http.Header{},
		Host:       u.Host,
	}
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Sec-WebSocket-Key", key)
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("User-Agent", userAgent)
	if err := req.Write(conn); err != nil {
		conn.Close()
		return nil, fmt.Errorf("datasportsapi: stream handshake: %w", err)
	}
	br := bufio.NewReaderSize(conn, 64<<10)
	res, err := http.ReadResponse(br, req)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("datasportsapi: stream handshake: %w", err)
	}
	if res.StatusCode != http.StatusSwitchingProtocols {
		res.Body.Close()
		conn.Close()
		return nil, fmt.Errorf("datasportsapi: stream handshake: the server answered %s", res.Status)
	}
	if !strings.EqualFold(res.Header.Get("Upgrade"), "websocket") || !headerHasToken(res.Header, "Connection", "upgrade") {
		conn.Close()
		return nil, errors.New("datasportsapi: stream handshake: the server did not upgrade to websocket")
	}
	if res.Header.Get("Sec-WebSocket-Accept") != acceptKey(key) {
		conn.Close()
		return nil, errors.New("datasportsapi: stream handshake: wrong Sec-WebSocket-Accept")
	}
	if ext := res.Header.Get("Sec-WebSocket-Extensions"); ext != "" {
		conn.Close()
		return nil, errors.New("datasportsapi: stream handshake: the server chose an extension nobody asked for")
	}
	_ = conn.SetDeadline(time.Time{})
	return newWSConn(conn, br, true, maxFrame), nil
}

func newWSConn(conn net.Conn, br *bufio.Reader, client bool, maxFrame int64) *wsConn {
	if maxFrame <= 0 {
		maxFrame = DefaultMaxFrame
	}
	return &wsConn{conn: conn, br: br, client: client, maxFrame: maxFrame}
}

func headerHasToken(h http.Header, name, token string) bool {
	for _, v := range h.Values(name) {
		for _, t := range strings.Split(v, ",") {
			if strings.EqualFold(strings.TrimSpace(t), token) {
				return true
			}
		}
	}
	return false
}

// writeFrame sends one frame (always FIN). Client frames are masked.
func (c *wsConn) writeFrame(op byte, payload []byte) error {
	c.wmu.Lock()
	defer c.wmu.Unlock()
	if c.closeSent {
		return net.ErrClosed
	}
	if op == opClose {
		c.closeSent = true
	}
	return c.writeFrameLocked(op, payload, true)
}

// writeFrameLocked writes a frame; the caller holds wmu (tests use it to send fragments).
func (c *wsConn) writeFrameLocked(op byte, payload []byte, fin bool) error {
	hdr := make([]byte, 0, 14)
	b0 := op
	if fin {
		b0 |= 0x80
	}
	hdr = append(hdr, b0)
	var maskBit byte
	if c.client {
		maskBit = 0x80
	}
	n := len(payload)
	switch {
	case n <= 125:
		hdr = append(hdr, maskBit|byte(n))
	case n <= 0xFFFF:
		hdr = append(hdr, maskBit|126, byte(n>>8), byte(n))
	default:
		hdr = append(hdr, maskBit|127)
		hdr = binary.BigEndian.AppendUint64(hdr, uint64(n))
	}
	data := payload
	if c.client {
		var mask [4]byte
		if _, err := rand.Read(mask[:]); err != nil {
			return err
		}
		hdr = append(hdr, mask[:]...)
		data = make([]byte, n)
		for i := range payload {
			data[i] = payload[i] ^ mask[i&3]
		}
	}
	if _, err := c.conn.Write(append(hdr, data...)); err != nil {
		return err
	}
	return nil
}

func (c *wsConn) writeText(b []byte) error { return c.writeFrame(opText, b) }

// writeClose sends a close frame (once).
func (c *wsConn) writeClose(code int, reason string) error {
	p := make([]byte, 2, 2+len(reason))
	binary.BigEndian.PutUint16(p, uint16(code))
	p = append(p, reason...)
	if len(p) > 125 {
		p = p[:125]
	}
	return c.writeFrame(opClose, p)
}

type wsFrame struct {
	fin     bool
	op      byte
	payload []byte
}

func (c *wsConn) readFrame() (wsFrame, error) {
	var h [2]byte
	if _, err := io.ReadFull(c.br, h[:]); err != nil {
		return wsFrame{}, err
	}
	f := wsFrame{fin: h[0]&0x80 != 0, op: h[0] & 0x0F}
	if h[0]&0x70 != 0 {
		return f, &wsProtoError{code: closeProtocolError, reason: "reserved bits set"}
	}
	masked := h[1]&0x80 != 0
	if masked == c.client {
		// A client must receive unmasked frames, a server masked ones.
		return f, &wsProtoError{code: closeProtocolError, reason: "wrong masking"}
	}
	n := int64(h[1] & 0x7F)
	switch n {
	case 126:
		var b [2]byte
		if _, err := io.ReadFull(c.br, b[:]); err != nil {
			return f, err
		}
		n = int64(binary.BigEndian.Uint16(b[:]))
	case 127:
		var b [8]byte
		if _, err := io.ReadFull(c.br, b[:]); err != nil {
			return f, err
		}
		u := binary.BigEndian.Uint64(b[:])
		if u > 1<<62 {
			return f, errFrameTooBig
		}
		n = int64(u)
	}
	if f.op >= 0x8 && (n > 125 || !f.fin) {
		return f, &wsProtoError{code: closeProtocolError, reason: "bad control frame"}
	}
	if n > c.maxFrame {
		return f, errFrameTooBig
	}
	var mask [4]byte
	if masked {
		if _, err := io.ReadFull(c.br, mask[:]); err != nil {
			return f, err
		}
	}
	f.payload = make([]byte, n)
	if _, err := io.ReadFull(c.br, f.payload); err != nil {
		return f, err
	}
	if masked {
		for i := range f.payload {
			f.payload[i] ^= mask[i&3]
		}
	}
	return f, nil
}

// readMessage returns the next text or binary message, answering pings and
// assembling fragments on the way. A close frame from the peer is answered and
// returned as *CloseError.
func (c *wsConn) readMessage() (op byte, msg []byte, err error) {
	var buf []byte
	started := false
	for {
		f, err := c.readFrame()
		if err != nil {
			return 0, nil, err
		}
		switch f.op {
		case opPing:
			if err := c.writeFrame(opPong, f.payload); err != nil && !errors.Is(err, net.ErrClosed) {
				return 0, nil, err
			}
			continue
		case opPong:
			continue
		case opClose:
			ce := &CloseError{Code: closeNoStatus}
			if len(f.payload) >= 2 {
				ce.Code = int(binary.BigEndian.Uint16(f.payload))
				ce.Reason = string(f.payload[2:])
			}
			echo := ce.Code
			if echo == closeNoStatus {
				echo = closeNormal
			}
			_ = c.writeClose(echo, "")
			return 0, nil, ce
		case opText, opBinary:
			if started {
				return 0, nil, &wsProtoError{code: closeProtocolError, reason: "new message inside a fragmented one"}
			}
			started = true
			op = f.op
			buf = f.payload
		case opContinuation:
			if !started {
				return 0, nil, &wsProtoError{code: closeProtocolError, reason: "continuation without a message"}
			}
			if int64(len(buf))+int64(len(f.payload)) > c.maxFrame {
				return 0, nil, errFrameTooBig
			}
			buf = append(buf, f.payload...)
		default:
			return 0, nil, &wsProtoError{code: closeProtocolError, reason: "unknown opcode"}
		}
		if f.fin {
			if op == opText && !utf8.Valid(buf) {
				return 0, nil, &wsProtoError{code: 1007, reason: "invalid UTF-8"}
			}
			return op, buf, nil
		}
	}
}

func (c *wsConn) close() error { return c.conn.Close() }
