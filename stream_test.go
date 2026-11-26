package datasportsapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeStream is a local API: POST /v2/stream/ticket mints tickets and /ws upgrades
// to a WebSocket whose server side the test drives through srvConn.
type fakeStream struct {
	t     *testing.T
	srv   *httptest.Server
	conns chan *srvConn

	mu           sync.Mutex
	tickets      int
	ticketStatus int
	last         *srvConn
}

type srvConn struct {
	t      *testing.T
	ws     *wsConn
	ticket string
	auth   string
	query  string
}

func newFakeStream(t *testing.T, tls bool) *fakeStream {
	t.Helper()
	fs := &fakeStream{t: t, conns: make(chan *srvConn, 8)}
	h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/v2/stream/ticket":
			if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+testKey {
				writeJSON(w, 401, apiErrBody("unauthorized", "bad key"))
				return
			}
			fs.mu.Lock()
			status := fs.ticketStatus
			fs.tickets++
			n := fs.tickets
			fs.mu.Unlock()
			if status != 0 {
				writeJSON(w, status, apiErrBody("refused", "no ticket"))
				return
			}
			scheme := "ws"
			if tls {
				scheme = "wss"
			}
			writeJSON(w, 200, map[string]any{"data": map[string]any{
				"ticket": fmt.Sprintf("t%d", n), "url": scheme + "://" + r.Host + "/ws", "expiresInSeconds": 30,
			}})
		case "/ws":
			if !strings.EqualFold(r.Header.Get("Upgrade"), "websocket") || r.Header.Get("Sec-WebSocket-Version") != "13" {
				http.Error(w, "not a websocket request", 400)
				return
			}
			conn, brw, err := w.(http.Hijacker).Hijack()
			if err != nil {
				return
			}
			_, _ = fmt.Fprintf(brw, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: %s\r\n\r\n", acceptKey(r.Header.Get("Sec-WebSocket-Key")))
			_ = brw.Flush()
			fs.conns <- &srvConn{
				t: t, ws: newWSConn(conn, brw.Reader, false, DefaultMaxFrame),
				ticket: r.URL.Query().Get("ticket"), auth: r.Header.Get("Authorization"), query: r.URL.RawQuery,
			}
		default:
			http.NotFound(w, r)
		}
	})
	if tls {
		fs.srv = httptest.NewTLSServer(h)
	} else {
		fs.srv = httptest.NewServer(h)
	}
	t.Cleanup(fs.srv.Close)
	return fs
}

func (fs *fakeStream) client(opts ...Option) *Client {
	c := NewClient(append([]Option{WithAPIKey(testKey), WithBaseURL(fs.srv.URL)}, opts...)...)
	c.streamBackoff = 5 * time.Millisecond
	return c
}

func (fs *fakeStream) ticketCount() int {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.tickets
}

func (fs *fakeStream) accept() *srvConn {
	fs.t.Helper()
	select {
	case c := <-fs.conns:
		fs.t.Cleanup(func() { _ = c.ws.close() })
		fs.mu.Lock()
		fs.last = c
		fs.mu.Unlock()
		if c.auth != "" || strings.Contains(c.query, testKey) {
			fs.t.Errorf("the socket request carried the key (Authorization %q, query %q)", c.auth, c.query)
		}
		return c
	case <-time.After(5 * time.Second):
		fs.t.Fatal("no WebSocket connection arrived")
		return nil
	}
}

// closeStream closes s while the last server connection answers the close frame.
func (fs *fakeStream) closeStream(s *Stream) {
	fs.mu.Lock()
	c := fs.last
	fs.mu.Unlock()
	if c != nil {
		go func() {
			_ = c.ws.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
			for {
				if _, _, err := c.ws.readMessage(); err != nil {
					return
				}
			}
		}()
	}
	_ = s.Close()
}

func (c *srvConn) send(v any) {
	c.t.Helper()
	b, _ := json.Marshal(v)
	if err := c.ws.writeText(b); err != nil {
		c.t.Fatalf("server send: %v", err)
	}
}

// recv returns the next JSON message from the client, skipping heartbeat pings.
func (c *srvConn) recv() map[string]any {
	c.t.Helper()
	for {
		_ = c.ws.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, msg, err := c.ws.readMessage()
		if err != nil {
			c.t.Fatalf("server recv: %v", err)
		}
		var m map[string]any
		if err := json.Unmarshal(msg, &m); err != nil {
			c.t.Fatalf("server recv %q: %v", msg, err)
		}
		if m["type"] == "ping" {
			continue
		}
		return m
	}
}

// expectClose reads until the client's close frame and returns its code.
func (c *srvConn) expectClose() int {
	c.t.Helper()
	for {
		_ = c.ws.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		_, msg, err := c.ws.readMessage()
		var ce *CloseError
		if errors.As(err, &ce) {
			return ce.Code
		}
		if err != nil {
			c.t.Fatalf("waiting for a close frame: %v", err)
		}
		if !strings.Contains(string(msg), `"ping"`) {
			c.t.Fatalf("waiting for a close frame, got %s", msg)
		}
	}
}

// closeWith sends a close frame and waits for the client's answer.
func (c *srvConn) closeWith(code int, reason string) {
	c.t.Helper()
	_ = c.ws.writeClose(code, reason)
	_ = c.ws.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		if _, _, err := c.ws.readMessage(); err != nil {
			break
		}
	}
	_ = c.ws.close()
}

func welcome(replay bool, hbMs int) map[string]any {
	return map[string]any{"type": "welcome", "protocol": 5, "heartbeatMs": hbMs, "maxChannels": 10, "replay": replay}
}

func fx(id int, home float64) map[string]any {
	return map[string]any{
		"id": id, "sport": "football",
		"status":    map[string]any{"type": "inprogress", "phase": "live"},
		"round":     map[string]any{"round": 3},
		"home":      map[string]any{"id": 10 * id, "name": fmt.Sprintf("Home %d", id)},
		"away":      map[string]any{"id": 10*id + 1, "name": fmt.Sprintf("Away %d", id)},
		"homeScore": map[string]any{"current": home, "periods": map[string]any{"period1": home}},
		"awayScore": map[string]any{"current": 0, "periods": map[string]any{"period1": 0}},
	}
}

func nextFrame(t *testing.T, s *Stream) Frame {
	t.Helper()
	select {
	case f, ok := <-s.Frames():
		if !ok {
			t.Fatalf("the stream ended: %v", s.Err())
		}
		return f
	case <-time.After(5 * time.Second):
		t.Fatal("no frame arrived")
	}
	return Frame{}
}

func waitEnd(t *testing.T, s *Stream) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-s.Frames():
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("the stream did not end")
		}
	}
}

func ids(t *testing.T, s *Stream, ch string) string {
	t.Helper()
	fs, err := s.Fixtures(ch)
	if err != nil {
		t.Fatal(err)
	}
	var out []string
	for _, f := range fs {
		out = append(out, fmt.Sprint(f.ID))
	}
	return strings.Join(out, ",")
}

func expectMsg(t *testing.T, got map[string]any, want string) {
	t.Helper()
	b, _ := json.Marshal(got)
	if string(b) != want {
		t.Errorf("client sent %s, want %s", b, want)
	}
}

func TestStreamSubscribeSnapshotDeltas(t *testing.T) {
	fs := newFakeStream(t, false)
	s, err := fs.client().Stream(bg, []string{"livescores", "fixture:1"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc := fs.accept()
	if sc.ticket != "t1" {
		t.Errorf("ticket %q", sc.ticket)
	}
	sc.send(welcome(false, 30000))
	expectMsg(t, sc.recv(), `{"channel":"livescores","type":"subscribe"}`)
	expectMsg(t, sc.recv(), `{"channel":"fixture:1","type":"subscribe"}`)
	if f := nextFrame(t, s); f.Type != "welcome" {
		t.Fatalf("first frame %s", f.Type)
	}

	sc.send(map[string]any{"type": "snapshot", "channel": "livescores", "seq": 1, "ts": 1, "data": []any{fx(1, 0), fx(2, 1)}})
	sc.send(map[string]any{"type": "snapshot", "channel": "fixture:1", "seq": 1, "ts": 1, "data": fx(1, 0)})
	f := nextFrame(t, s)
	if snap, ok := f.Value.(*WsSnapshot); !ok || f.Channel != "livescores" || f.Seq == nil || *f.Seq != 1 || snap.Channel != "livescores" {
		t.Fatalf("snapshot frame %+v", f)
	}
	nextFrame(t, s)
	if got := ids(t, s, "livescores"); got != "1,2" {
		t.Errorf("held %q", got)
	}

	// A delta deep-merges: homeScore.current changes, homeScore.periods stays.
	sc.send(map[string]any{"type": "delta", "channel": "livescores", "seq": 2, "ts": 2, "id": 1, "sport": "football", "changes": map[string]any{"homeScore": map[string]any{"current": 2}}})
	f = nextFrame(t, s)
	d, ok := f.Value.(*WsDelta)
	if !ok || d.ID != 1 {
		t.Fatalf("delta frame %+v", f)
	}
	live, _ := s.Fixtures("livescores")
	if *live[0].HomeScore.Current != 2 || live[0].HomeScore.Periods["period1"] != 0 || live[0].Home.Name != "Home 1" {
		t.Errorf("after delta: %+v %+v", live[0].HomeScore, live[0].Home)
	}
	// The copy is the caller's: changing it does not touch the stream.
	live[0].Home.Name = "changed"
	if again, _ := s.Fixtures("livescores"); again[0].Home.Name != "Home 1" {
		t.Error("Fixtures returned shared state")
	}

	// fixture:{id} merges too.
	sc.send(map[string]any{"type": "delta", "channel": "fixture:1", "seq": 2, "ts": 2, "id": 1, "sport": "football", "changes": map[string]any{"homeScore": map[string]any{"current": 2}, "round": nil}})
	nextFrame(t, s)
	one, _ := s.Fixtures("fixture:1")
	if len(one) != 1 || *one[0].HomeScore.Current != 2 || one[0].Round != nil {
		t.Errorf("fixture:1 = %+v", one)
	}

	// A finished fixture leaves the list.
	sc.send(map[string]any{"type": "delta", "channel": "livescores", "seq": 3, "ts": 3, "id": 2, "sport": "football", "changes": map[string]any{"status": map[string]any{"type": "finished"}}})
	nextFrame(t, s)
	if got := ids(t, s, "livescores"); got != "1" {
		t.Errorf("after finished: %q", got)
	}
	// A fixture frame adds one.
	sc.send(map[string]any{"type": "fixture", "channel": "livescores", "seq": 4, "ts": 4, "id": 3, "sport": "football", "data": fx(3, 0)})
	nextFrame(t, s)
	if got := ids(t, s, "livescores"); got != "1,3" {
		t.Errorf("after fixture: %q", got)
	}

	// A gap (seq 4 → 6) asks for a resync; the new snapshot replaces the list.
	sc.send(map[string]any{"type": "delta", "channel": "livescores", "seq": 6, "ts": 6, "id": 1, "sport": "football", "changes": map[string]any{}})
	expectMsg(t, sc.recv(), `{"channel":"livescores","type":"resync"}`)
	nextFrame(t, s)
	sc.send(map[string]any{"type": "snapshot", "channel": "livescores", "seq": 20, "ts": 7, "data": []any{fx(9, 0)}})
	nextFrame(t, s)
	if got := ids(t, s, "livescores"); got != "9" {
		t.Errorf("after resync: %q", got)
	}
	// playerFrame has no seq and never triggers a resync; other channels keep no state.
	sc.send(map[string]any{"type": "playerFrame", "channel": "livescores", "ts": 8, "id": 9, "sport": "basketball", "frame": map[string]any{}})
	if f := nextFrame(t, s); f.Seq != nil || f.Type != "playerFrame" {
		t.Errorf("playerFrame %+v", f)
	}
	if _, err := s.Fixtures("odds:1"); !errors.Is(err, ErrNoState) {
		t.Errorf("odds state: %v", err)
	}

	// Unsubscribe drops the state; Subscribe goes out at once.
	if err := s.Unsubscribe("fixture:1"); err != nil {
		t.Fatal(err)
	}
	expectMsg(t, sc.recv(), `{"channel":"fixture:1","type":"unsubscribe"}`)
	if one, _ := s.Fixtures("fixture:1"); len(one) != 0 {
		t.Errorf("fixture:1 kept %d fixtures", len(one))
	}
	if err := s.Subscribe("league:17"); err != nil {
		t.Fatal(err)
	}
	expectMsg(t, sc.recv(), `{"channel":"league:17","type":"subscribe"}`)

	// Close: a close frame, Frames closes, Err stays nil.
	done := make(chan int)
	go func() { done <- sc.expectClose() }()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if code := <-done; code != 1000 {
		t.Errorf("close code %d", code)
	}
	waitEnd(t, s)
	if s.Err() != nil {
		t.Errorf("Err after Close = %v", s.Err())
	}
	if err := s.Subscribe("livescores"); !errors.Is(err, ErrStreamClosed) {
		t.Errorf("Subscribe after Close = %v", err)
	}
	_ = s.Close() // twice is fine
}

func TestStreamReconnect1012WithReplay(t *testing.T) {
	fs := newFakeStream(t, false)
	s, err := fs.client().Stream(bg, []string{"livescores"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.closeStream(s)
	sc := fs.accept()
	sc.send(welcome(true, 30000))
	expectMsg(t, sc.recv(), `{"channel":"livescores","type":"subscribe"}`)
	sc.send(map[string]any{"type": "snapshot", "channel": "livescores", "seq": 5, "ts": 1, "data": []any{fx(1, 0)}})
	sc.send(map[string]any{"type": "delta", "channel": "livescores", "seq": 6, "ts": 2, "id": 1, "sport": "football", "changes": map[string]any{"homeScore": map[string]any{"current": 1}}})
	for range 3 {
		nextFrame(t, s)
	}
	sc.closeWith(1012, "server restarting; reconnect")

	sc2 := fs.accept()
	if sc2.ticket != "t2" || fs.ticketCount() != 2 {
		t.Errorf("second connection used ticket %q (%d minted)", sc2.ticket, fs.ticketCount())
	}
	sc2.send(welcome(true, 30000))
	expectMsg(t, sc2.recv(), `{"channel":"livescores","lastSeq":6,"type":"subscribe"}`)
	// Replayed frames apply on top of what the stream held.
	sc2.send(map[string]any{"type": "delta", "channel": "livescores", "seq": 7, "ts": 3, "id": 1, "sport": "football", "changes": map[string]any{"homeScore": map[string]any{"current": 2}}})
	sc2.send(map[string]any{"type": "subscribed", "channel": "livescores", "seq": 7, "ts": 3, "replayed": 1})
	for {
		if f := nextFrame(t, s); f.Type == "subscribed" {
			break
		}
	}
	live, _ := s.Fixtures("livescores")
	if len(live) != 1 || *live[0].HomeScore.Current != 2 {
		t.Errorf("after replay: %+v", live)
	}
}

func TestStream1008(t *testing.T) {
	t.Run("fatal", func(t *testing.T) {
		fs := newFakeStream(t, false)
		s, err := fs.client().Stream(bg, []string{"livescores"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		sc := fs.accept()
		sc.send(welcome(false, 30000))
		sc.recv()
		sc.closeWith(1008, "api key revoked")
		waitEnd(t, s)
		var ce *CloseError
		if !errors.As(s.Err(), &ce) || ce.Code != 1008 || fs.ticketCount() != 1 {
			t.Errorf("Err = %v after %d tickets", s.Err(), fs.ticketCount())
		}
	})
	t.Run("reconnect", func(t *testing.T) {
		fs := newFakeStream(t, false)
		s, err := fs.client().Stream(bg, []string{"livescores"}, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer fs.closeStream(s)
		sc := fs.accept()
		sc.send(welcome(false, 30000))
		sc.recv()
		sc.closeWith(1008, "plan changed; reconnect")
		sc2 := fs.accept()
		sc2.send(welcome(false, 30000))
		expectMsg(t, sc2.recv(), `{"channel":"livescores","type":"subscribe"}`)
	})
}

func TestStream1013Fatal(t *testing.T) {
	fs := newFakeStream(t, false)
	s, err := fs.client().Stream(bg, []string{"livescores"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc := fs.accept()
	sc.closeWith(1013, "too many connections")
	waitEnd(t, s)
	var ce *CloseError
	if !errors.As(s.Err(), &ce) || ce.Code != 1013 {
		t.Errorf("Err = %v", s.Err())
	}
}

func TestStreamTicketErrors(t *testing.T) {
	fs := newFakeStream(t, false)
	fs.ticketStatus = 403
	if _, err := fs.client().Stream(bg, []string{"livescores"}, nil); !errors.Is(err, ErrForbidden) {
		t.Errorf("first ticket 403: %v", err)
	}

	// A dropped socket reconnects; a 401 on the new ticket ends the stream.
	fs = newFakeStream(t, false)
	s, err := fs.client().Stream(bg, []string{"livescores"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc := fs.accept()
	fs.mu.Lock()
	fs.ticketStatus = 401
	fs.mu.Unlock()
	_ = sc.ws.close()
	waitEnd(t, s)
	if !errors.Is(s.Err(), ErrUnauthorized) {
		t.Errorf("Err = %v", s.Err())
	}
}

func TestStreamDroppedSocketReconnects(t *testing.T) {
	fs := newFakeStream(t, false)
	s, err := fs.client().Stream(bg, []string{"sport:tennis"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.closeStream(s)
	sc := fs.accept()
	sc.send(welcome(false, 30000))
	sc.recv()
	_ = sc.ws.close() // no close frame: 1006
	sc2 := fs.accept()
	sc2.send(welcome(false, 30000))
	expectMsg(t, sc2.recv(), `{"channel":"sport:tennis","type":"subscribe"}`)
}

func TestStreamFrameTooBig(t *testing.T) {
	fs := newFakeStream(t, false)
	c := fs.client()
	c.wsMaxFrame = 1024
	s, err := c.Stream(bg, []string{"livescores"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.closeStream(s)
	sc := fs.accept()
	sc.send(map[string]any{"type": "error", "message": strings.Repeat("x", 2048)})
	if code := sc.expectClose(); code != 1009 {
		t.Errorf("close code %d, want 1009", code)
	}
	fs.accept() // and it reconnects
}

func TestStreamPingPongAndFragments(t *testing.T) {
	fs := newFakeStream(t, false)
	s, err := fs.client().Stream(bg, []string{"livescores"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.closeStream(s)
	sc := fs.accept()
	// A control ping is answered with a pong carrying the same payload.
	if err := sc.ws.writeFrame(opPing, []byte("hi")); err != nil {
		t.Fatal(err)
	}
	_ = sc.ws.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	pf, err := sc.ws.readFrame()
	if err != nil || pf.op != opPong || string(pf.payload) != "hi" {
		t.Fatalf("pong %+v %v", pf, err)
	}
	// A message in three fragments, with a ping between them, arrives whole.
	msg, _ := json.Marshal(welcome(false, 30000))
	sc.ws.wmu.Lock()
	_ = sc.ws.writeFrameLocked(opText, msg[:5], false)
	_ = sc.ws.writeFrameLocked(opPing, nil, true)
	_ = sc.ws.writeFrameLocked(opContinuation, msg[5:10], false)
	_ = sc.ws.writeFrameLocked(opContinuation, msg[10:], true)
	sc.ws.wmu.Unlock()
	if f := nextFrame(t, s); f.Type != "welcome" {
		t.Errorf("fragmented frame %+v", f)
	}
	// The app-level heartbeat follows heartbeatMs.
	sc.send(welcome(false, 20))
	_ = sc.ws.conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	for {
		_, m, err := sc.ws.readMessage()
		if err != nil {
			t.Fatal(err)
		}
		if string(m) == `{"type":"ping"}` {
			break
		}
	}
}

func TestStreamContextEnds(t *testing.T) {
	fs := newFakeStream(t, false)
	ctx, cancel := context.WithCancel(bg)
	s, err := fs.client().Stream(ctx, []string{"livescores"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sc := fs.accept()
	go sc.expectClose()
	cancel()
	waitEnd(t, s)
	if !errors.Is(s.Err(), context.Canceled) {
		t.Errorf("Err = %v", s.Err())
	}
}

func TestStreamOverTLS(t *testing.T) {
	fs := newFakeStream(t, true)
	c := fs.client(WithHTTPClient(fs.srv.Client()))
	s, err := c.Stream(bg, []string{"livescores"}, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer fs.closeStream(s)
	sc := fs.accept()
	sc.send(welcome(false, 30000))
	expectMsg(t, sc.recv(), `{"channel":"livescores","type":"subscribe"}`)
	if f := nextFrame(t, s); f.Type != "welcome" {
		t.Errorf("frame %+v", f)
	}
}

func TestStreamURLRules(t *testing.T) {
	remote := NewClient(WithAPIKey(testKey), WithBaseURL("https://api.example.com"))
	local := NewClient(WithAPIKey(testKey), WithBaseURL("http://127.0.0.1:1"))
	cases := []struct {
		c    *Client
		raw  string
		want string
	}{
		{remote, "wss://ws.example.com/", "wss://ws.example.com/?ticket=t9"},
		{remote, "wss://ws.example.com/?ticket=given", "wss://ws.example.com/?ticket=given"},
		{remote, "ws://ws.example.com/", ""},
		{remote, "https://ws.example.com/", ""},
		{local, "ws://127.0.0.1:1/ws", "ws://127.0.0.1:1/ws?ticket=t9"},
	}
	for _, tc := range cases {
		got, err := tc.c.streamURL("t9", tc.raw)
		if (err != nil) != (tc.want == "") || got != tc.want {
			t.Errorf("%s: %q %v", tc.raw, got, err)
		}
	}
}

func TestMergeInto(t *testing.T) {
	dst := map[string]any{"a": map[string]any{"b": 1, "c": 2}, "d": 3, "e": map[string]any{"f": 1}}
	mergeInto(dst, map[string]any{"a": map[string]any{"b": 9}, "d": nil, "e": 5, "g": map[string]any{"h": 1}})
	b, _ := json.Marshal(dst)
	if string(b) != `{"a":{"b":9,"c":2},"d":null,"e":5,"g":{"h":1}}` {
		t.Errorf("merged %s", b)
	}
}
