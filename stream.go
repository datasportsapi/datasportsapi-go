package datasportsapi

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Frame is one message of the live stream, passed on as the server sent it.
type Frame struct {
	// Type is the frame's type: welcome, snapshot, delta, fixture, subscribed,
	// events, odds, error, pong, …
	Type string
	// Channel is the channel it belongs to; empty for welcome and pong.
	Channel string
	// Seq is the frame's sequence number on its channel; nil when it has none.
	Seq *int64
	// Raw is the frame's JSON.
	Raw json.RawMessage
	// Value is the frame decoded into its struct (*WsDelta, *WsSnapshot, …); nil for
	// a type this version does not know or a frame that did not decode.
	Value any
}

// StreamOptions configures Client.Stream. A nil *StreamOptions takes the defaults.
type StreamOptions struct {
	// FrameBuffer is how many frames may wait for the reader of Frames before the
	// stream stops reading the socket (default 256).
	FrameBuffer int
}

// Stream is a live WebSocket connection that holds a current copy of the fixtures
// on its channels. It mints its own single-use tickets, subscribes, resyncs a
// channel after a gap and reconnects on its own, replaying missed frames where
// the plan allows. Read it with Frames; end it with Close.
type Stream struct {
	c      *Client
	ctx    context.Context
	cancel context.CancelFunc
	frames chan Frame
	done   chan struct{}

	mu          sync.Mutex
	conn        *wsConn
	welcomed    bool
	replay      bool
	subs        []string
	seq         map[string]int64
	lists       map[string]*fixtureList
	singles     map[string]map[string]any
	err         error
	closeCalled bool
	ended       bool

	// Touched by the run goroutine only.
	attempt int
}

type fixtureList struct {
	order []string
	byID  map[string]map[string]any
}

const (
	kindNone = iota
	kindList
	kindSingle
)

func channelKind(ch string) int {
	switch {
	case ch == "livescores", strings.HasPrefix(ch, "sport:"), strings.HasPrefix(ch, "league:"), strings.HasPrefix(ch, "team:"):
		return kindList
	case strings.HasPrefix(ch, "fixture:"):
		return kindSingle
	}
	return kindNone
}

// Stream opens the live stream (Business plan and up) and subscribes to channels
// (livescores, sport:football, fixture:237609, …). It returns once the socket is
// open; ticket errors (401, 403) and a failed first connection are returned here.
// The stream ends when Close is called, when ctx ends, or on a fatal error (Err).
func (c *Client) Stream(ctx context.Context, channels []string, opts *StreamOptions) (*Stream, error) {
	if c.initErr != nil {
		return nil, c.initErr
	}
	if ctx == nil {
		ctx = context.Background()
	}
	buf := 256
	if opts != nil && opts.FrameBuffer > 0 {
		buf = opts.FrameBuffer
	}
	sctx, cancel := context.WithCancel(ctx)
	s := &Stream{
		c:       c,
		ctx:     sctx,
		cancel:  cancel,
		frames:  make(chan Frame, buf),
		done:    make(chan struct{}),
		seq:     map[string]int64{},
		lists:   map[string]*fixtureList{},
		singles: map[string]map[string]any{},
	}
	for _, ch := range channels {
		s.addSub(ch)
	}
	conn, err := s.connect()
	if err != nil {
		cancel()
		return nil, err
	}
	go s.watch()
	go s.run(conn)
	return s, nil
}

// Frames returns the frames as they arrive, after the stream has applied them to
// its fixtures. The channel closes when the stream ends; Err then tells why.
func (s *Stream) Frames() <-chan Frame { return s.frames }

// Err returns the error that ended the stream: nil after Close, the context's
// error when ctx ended, else the fatal error (a *CloseError for 1008 or 1013, an
// *APIError when a new ticket was refused).
func (s *Stream) Err() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.err
}

// Close ends the stream: it sends a close frame, waits briefly for the server's
// answer and closes Frames. It is safe to call more than once.
func (s *Stream) Close() error {
	s.mu.Lock()
	if !s.ended {
		s.closeCalled = true
	}
	s.mu.Unlock()
	s.cancel()
	<-s.done
	return nil
}

// Subscribe adds a channel. On an open connection it is sent at once; otherwise
// it is sent after the next welcome.
func (s *Stream) Subscribe(channel string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended || s.ctx.Err() != nil {
		return ErrStreamClosed
	}
	if !s.addSub(channel) {
		return nil
	}
	if s.conn != nil && s.welcomed {
		return s.send(s.conn, map[string]any{"type": "subscribe", "channel": channel})
	}
	return nil
}

// Unsubscribe leaves a channel and drops what the stream held for it.
func (s *Stream) Unsubscribe(channel string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended || s.ctx.Err() != nil {
		return ErrStreamClosed
	}
	i := s.subIndex(channel)
	if i < 0 {
		return nil
	}
	s.subs = append(s.subs[:i], s.subs[i+1:]...)
	delete(s.seq, channel)
	delete(s.lists, channel)
	delete(s.singles, channel)
	if s.conn != nil && s.welcomed {
		return s.send(s.conn, map[string]any{"type": "unsubscribe", "channel": channel})
	}
	return nil
}

// Fixtures returns a copy of the fixtures the stream holds for a fixture channel,
// every delta applied: in list order for livescores, sport:*, league:* and team:*;
// one fixture (or none) for fixture:{id}. Other channels keep no fixtures
// (ErrNoState).
func (s *Stream) Fixtures(channel string) ([]Fixture, error) {
	kind := channelKind(channel)
	if kind == kindNone {
		return nil, ErrNoState
	}
	s.mu.Lock()
	var held []any
	switch kind {
	case kindList:
		if l := s.lists[channel]; l != nil {
			for _, id := range l.order {
				held = append(held, l.byID[id])
			}
		}
	case kindSingle:
		if f := s.singles[channel]; f != nil {
			held = append(held, f)
		}
	}
	raw, err := json.Marshal(held)
	s.mu.Unlock()
	out := []Fixture{}
	if err != nil {
		return out, fmt.Errorf("datasportsapi: copying the fixtures: %w", err)
	}
	if len(held) == 0 {
		return out, nil
	}
	if err := json.Unmarshal(raw, &out); err != nil {
		return out, fmt.Errorf("datasportsapi: decoding the held fixtures: %w", err)
	}
	return out, nil
}

/* ------------------------------------------------------------- internals */

func (s *Stream) subIndex(ch string) int {
	for i, x := range s.subs {
		if x == ch {
			return i
		}
	}
	return -1
}

// addSub records a channel; false when it was already there. Caller holds mu (or owns s).
func (s *Stream) addSub(ch string) bool {
	if ch == "" || s.subIndex(ch) >= 0 {
		return false
	}
	s.subs = append(s.subs, ch)
	return true
}

func (s *Stream) send(conn *wsConn, v any) error {
	b, err := json.Marshal(v)
	if err != nil {
		return err
	}
	return conn.writeText(b)
}

// connect mints a ticket through the normal request path and opens the socket.
func (s *Stream) connect() (*wsConn, error) {
	var t StreamTicketResponse
	if err := s.c.do(s.ctx, http.MethodPost, "/v2/stream/ticket", nil, nil, &t); err != nil {
		return nil, err
	}
	u, err := s.c.streamURL(t.Data.Ticket, t.Data.URL)
	if err != nil {
		return nil, err
	}
	return dialWS(s.ctx, u, s.c.tlsConfig(), s.c.wsMaxFrame, s.c.timeout)
}

// streamURL checks the socket URL a ticket came with and adds ?ticket= when it lacks one.
func (c *Client) streamURL(ticket, raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return "", fmt.Errorf("datasportsapi: the ticket's stream URL %q is not usable", raw)
	}
	switch u.Scheme {
	case "wss":
	case "ws":
		base, _ := url.Parse(c.baseURL)
		if base == nil || !isLocalHost(base.Hostname()) {
			return "", fmt.Errorf("datasportsapi: refusing a ws:// stream URL for the non-local base URL %s", c.baseURL)
		}
	default:
		return "", fmt.Errorf("datasportsapi: the stream URL scheme %q is not wss", u.Scheme)
	}
	q := u.Query()
	if q.Get("ticket") == "" {
		if ticket == "" {
			return "", errors.New("datasportsapi: the ticket answer carried no ticket")
		}
		q.Set("ticket", ticket)
		u.RawQuery = q.Encode()
	}
	return u.String(), nil
}

func (c *Client) tlsConfig() *tls.Config {
	if tr, ok := c.http.Transport.(*http.Transport); ok && tr.TLSClientConfig != nil {
		return tr.TLSClientConfig.Clone()
	}
	return nil
}

// watch closes the current connection politely once the stream's context ends.
func (s *Stream) watch() {
	select {
	case <-s.ctx.Done():
	case <-s.done:
		return
	}
	s.mu.Lock()
	if s.conn != nil {
		_ = s.conn.writeClose(closeNormal, "")
		_ = s.conn.conn.SetReadDeadline(time.Now().Add(time.Second))
	}
	s.mu.Unlock()
}

func (s *Stream) backoff() time.Duration {
	base := s.c.streamBackoff
	if base <= 0 {
		base = time.Second
	}
	limit := 30 * base
	d := base << min(s.attempt, 10)
	if d > limit || d <= 0 {
		d = limit
	}
	s.attempt++
	// Full delay at most, half of it at least.
	return d/2 + rand.N(d/2+1)
}

func (s *Stream) run(conn *wsConn) {
	defer func() {
		s.mu.Lock()
		s.ended = true
		if s.err == nil && !s.closeCalled {
			// The caller's context ended (Close leaves Err nil).
			s.err = s.ctx.Err()
		}
		s.mu.Unlock()
		close(s.frames)
		close(s.done)
	}()
	for {
		err := s.session(conn)
		if s.ctx.Err() != nil {
			return
		}
		delay, fatal := s.decide(err)
		if fatal != nil {
			s.fail(fatal)
			return
		}
		for {
			if sleepCtx(s.ctx, delay) != nil {
				return
			}
			c, err := s.connect()
			if err == nil {
				conn = c
				break
			}
			if s.ctx.Err() != nil {
				return
			}
			var apiErr *APIError
			if errors.As(err, &apiErr) && (apiErr.Status == http.StatusUnauthorized || apiErr.Status == http.StatusForbidden) {
				s.fail(err)
				return
			}
			delay = s.backoff()
		}
	}
}

func (s *Stream) fail(err error) {
	s.mu.Lock()
	s.err = err
	s.mu.Unlock()
	s.cancel()
}

// decide maps how a connection ended to the next step (sdk/notes.md → Stream 6).
func (s *Stream) decide(err error) (time.Duration, error) {
	var ce *CloseError
	if errors.As(err, &ce) {
		switch ce.Code {
		case 1012:
			return 0, nil
		case 1008:
			if strings.Contains(strings.ToLower(ce.Reason), "reconnect") {
				return 0, nil
			}
			return 0, ce
		case 1013:
			return 0, ce
		}
	}
	return s.backoff(), nil
}

// session reads one connection until it ends and returns why.
func (s *Stream) session(conn *wsConn) error {
	s.mu.Lock()
	s.conn = conn
	s.welcomed = false
	s.mu.Unlock()
	if s.ctx.Err() != nil {
		// The context ended while connecting; watch may have missed this connection.
		_ = conn.writeClose(closeNormal, "")
		_ = conn.conn.SetReadDeadline(time.Now().Add(time.Second))
	}

	hb := 30 * time.Second
	hbCh := make(chan time.Duration, 1)
	stop := make(chan struct{})
	go pinger(conn, hbCh, stop)
	defer func() {
		close(stop)
		s.mu.Lock()
		s.conn = nil
		s.welcomed = false
		s.mu.Unlock()
		_ = conn.close()
	}()

	for {
		s.mu.Lock()
		if s.ctx.Err() == nil {
			_ = conn.conn.SetReadDeadline(time.Now().Add(2*hb + 5*time.Second))
		}
		s.mu.Unlock()
		_, msg, err := conn.readMessage()
		if err != nil {
			var pe *wsProtoError
			var ce *CloseError
			switch {
			case errors.Is(err, errFrameTooBig):
				_ = conn.writeClose(closeTooBig, "frame too big")
			case errors.As(err, &pe):
				_ = conn.writeClose(pe.code, pe.reason)
			case errors.As(err, &ce):
				return ce
			}
			return &CloseError{Code: closeAbnormal, Reason: err.Error()}
		}
		if d, ok := s.handle(conn, msg); ok && d != hb {
			hb = d
			select {
			case hbCh <- d:
			default:
			}
		}
	}
}

func pinger(conn *wsConn, hbCh <-chan time.Duration, stop <-chan struct{}) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case d := <-hbCh:
			t.Reset(d)
		case <-t.C:
			if conn.writeText([]byte(`{"type":"ping"}`)) != nil {
				return
			}
		}
	}
}

// handle applies one frame and passes it on. It returns the heartbeat interval a
// welcome announced.
func (s *Stream) handle(conn *wsConn, msg []byte) (time.Duration, bool) {
	var head struct {
		Type    string      `json:"type"`
		Channel string      `json:"channel"`
		Seq     json.Number `json:"seq"`
	}
	f := Frame{Raw: append(json.RawMessage(nil), msg...)}
	if json.Unmarshal(msg, &head) == nil {
		f.Type, f.Channel = head.Type, head.Channel
		if n, ok := numberToInt(head.Seq); ok {
			f.Seq = &n
		}
		if mk := wsFrameTypes[head.Type]; mk != nil {
			v := mk()
			if json.Unmarshal(msg, v) == nil {
				f.Value = v
			}
		}
	}

	var hb time.Duration
	var hbOK bool
	if f.Type == "welcome" {
		hb, hbOK = s.welcome(conn, msg)
	} else if f.Channel != "" {
		s.apply(conn, f, msg)
	}

	select {
	case s.frames <- f:
	case <-s.ctx.Done():
	}
	return hb, hbOK
}

func numberToInt(n json.Number) (int64, bool) {
	if n == "" {
		return 0, false
	}
	if i, err := strconv.ParseInt(string(n), 10, 64); err == nil {
		return i, true
	}
	if f, err := strconv.ParseFloat(string(n), 64); err == nil {
		return int64(f), true
	}
	return 0, false
}

// welcome subscribes every channel, with lastSeq where the server replays.
func (s *Stream) welcome(conn *wsConn, msg []byte) (time.Duration, bool) {
	var w struct {
		HeartbeatMs json.Number `json:"heartbeatMs"`
		Replay      bool        `json:"replay"`
	}
	_ = json.Unmarshal(msg, &w)
	hb := 30 * time.Second
	if ms, ok := numberToInt(w.HeartbeatMs); ok && ms > 0 {
		hb = time.Duration(ms) * time.Millisecond
	}
	s.attempt = 0
	s.mu.Lock()
	defer s.mu.Unlock()
	s.welcomed = true
	s.replay = w.Replay
	for _, ch := range s.subs {
		m := map[string]any{"type": "subscribe", "channel": ch}
		if last, ok := s.seq[ch]; ok && w.Replay {
			m["lastSeq"] = last
		}
		if err := s.send(conn, m); err != nil {
			break
		}
	}
	return hb, true
}

var droppedStatus = map[string]bool{"finished": true, "canceled": true, "postponed": true}

func decodeJSON(raw []byte) (any, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	var v any
	err := d.Decode(&v)
	return v, err
}

// apply checks a frame's seq and updates the fixtures (sdk/notes.md → Stream 3, 4).
func (s *Stream) apply(conn *wsConn, f Frame, msg []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ch := f.Channel
	if s.subIndex(ch) < 0 {
		return
	}
	if f.Seq != nil {
		switch f.Type {
		case "snapshot", "subscribed":
			s.seq[ch] = *f.Seq
		default:
			last, ok := s.seq[ch]
			if ok && *f.Seq <= last {
				return // already applied (a replay overlapping what we hold)
			}
			if ok && *f.Seq > last+1 {
				_ = s.send(conn, map[string]any{"type": "resync", "channel": ch})
			}
			s.seq[ch] = *f.Seq
		}
	}
	kind := channelKind(ch)
	if kind == kindNone {
		return
	}
	switch f.Type {
	case "snapshot", "fixture", "delta":
	default:
		return
	}
	v, err := decodeJSON(msg)
	if err != nil {
		return
	}
	frame, _ := v.(map[string]any)
	switch f.Type {
	case "snapshot":
		if kind == kindList {
			l := &fixtureList{byID: map[string]map[string]any{}}
			arr, _ := frame["data"].([]any)
			for _, x := range arr {
				if fx, ok := x.(map[string]any); ok {
					l.upsert(fx)
				}
			}
			s.lists[ch] = l
		} else {
			fx, _ := frame["data"].(map[string]any)
			s.singles[ch] = fx
		}
	case "fixture":
		fx, _ := frame["data"].(map[string]any)
		if fx == nil {
			return
		}
		if kind == kindList {
			l := s.lists[ch]
			if l == nil {
				l = &fixtureList{byID: map[string]map[string]any{}}
				s.lists[ch] = l
			}
			l.upsert(fx)
		} else {
			s.singles[ch] = fx
		}
	case "delta":
		changes, _ := frame["changes"].(map[string]any)
		if changes == nil {
			return
		}
		id := idKey(frame["id"])
		if kind == kindList {
			l := s.lists[ch]
			if l == nil {
				return
			}
			fx := l.byID[id]
			if fx == nil {
				return
			}
			mergeInto(fx, changes)
			if st, _ := fx["status"].(map[string]any); st != nil {
				if t, _ := st["type"].(string); droppedStatus[t] {
					l.remove(id)
				}
			}
		} else if fx := s.singles[ch]; fx != nil {
			mergeInto(fx, changes)
		}
	}
}

func idKey(v any) string {
	switch x := v.(type) {
	case json.Number:
		if i, ok := numberToInt(x); ok {
			return strconv.FormatInt(i, 10)
		}
		return x.String()
	case string:
		return x
	}
	return fmt.Sprint(v)
}

func (l *fixtureList) upsert(fx map[string]any) {
	id := idKey(fx["id"])
	if _, ok := l.byID[id]; !ok {
		l.order = append(l.order, id)
	}
	l.byID[id] = fx
}

func (l *fixtureList) remove(id string) {
	if _, ok := l.byID[id]; !ok {
		return
	}
	delete(l.byID, id)
	for i, x := range l.order {
		if x == id {
			l.order = append(l.order[:i], l.order[i+1:]...)
			break
		}
	}
}

// mergeInto deep-merges patch into dst: objects merge key by key; any other value,
// null included, overwrites.
func mergeInto(dst, patch map[string]any) {
	for k, v := range patch {
		if pm, ok := v.(map[string]any); ok {
			if dm, ok := dst[k].(map[string]any); ok {
				mergeInto(dm, pm)
				continue
			}
			cp := map[string]any{}
			mergeInto(cp, pm)
			dst[k] = cp
			continue
		}
		dst[k] = v
	}
}
