package datasportsapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const testKey = "test-key-not-real"

var bg = context.Background()

// testClient points a client at an httptest server and records the waits between
// attempts instead of sleeping.
type testClient struct {
	*Client
	srv   *httptest.Server
	mu    sync.Mutex
	waits []time.Duration
}

func newTestClient(t *testing.T, h http.HandlerFunc, opts ...Option) *testClient {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	tc := &testClient{srv: srv}
	tc.Client = NewClient(append([]Option{WithAPIKey(testKey), WithBaseURL(srv.URL)}, opts...)...)
	tc.Client.sleep = func(ctx context.Context, d time.Duration) error {
		tc.mu.Lock()
		tc.waits = append(tc.waits, d)
		tc.mu.Unlock()
		return ctx.Err()
	}
	return tc
}

func (tc *testClient) recordedWaits() []time.Duration {
	tc.mu.Lock()
	defer tc.mu.Unlock()
	return append([]time.Duration(nil), tc.waits...)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func apiErrBody(code, msg string) map[string]any {
	return map[string]any{"error": map[string]any{"code": code, "message": msg}}
}

func TestHeadersQueryAndDecoding(t *testing.T) {
	var got *http.Request
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		got = r.Clone(context.Background())
		writeJSON(w, 200, map[string]any{
			"data": []any{map[string]any{"id": 5, "sport": "football", "unknownField": map[string]any{"x": 1}, "home": map[string]any{"id": 1, "name": "Home FC"}}},
			"meta": map[string]any{"date": "2026-11-26", "page": 0, "hasNextPage": false, "brandNewMeta": true},
		})
	})
	res, err := tc.Fixtures.List(bg, &FixturesListParams{Date: "2026-11-26", Sport: Ptr("football"), League: Ptr(int64(17))})
	if err != nil {
		t.Fatal(err)
	}
	if got.Header.Get("Authorization") != "Bearer "+testKey {
		t.Errorf("Authorization = %q", got.Header.Get("Authorization"))
	}
	if got.Header.Get("Accept") != "application/json" {
		t.Errorf("Accept = %q", got.Header.Get("Accept"))
	}
	if got.Header.Get("User-Agent") != "datasportsapi-go/"+Version || Version != "0.1.0" {
		t.Errorf("User-Agent = %q", got.Header.Get("User-Agent"))
	}
	if got.Header.Get("Content-Type") != "" {
		t.Errorf("a GET sent Content-Type %q", got.Header.Get("Content-Type"))
	}
	if got.Method != "GET" || got.URL.Path != "/v2/fixtures" {
		t.Errorf("request = %s %s", got.Method, got.URL.Path)
	}
	if q := got.URL.RawQuery; q != "date=2026-11-26&league=17&sport=football" {
		t.Errorf("query = %q (nil fields must be left out)", q)
	}
	if strings.Contains(got.URL.String(), testKey) {
		t.Error("the key is in the URL")
	}
	if len(res.Data) != 1 || res.Data[0].ID != 5 || res.Data[0].Home.Name != "Home FC" {
		t.Errorf("data = %+v", res.Data)
	}
	if res.Meta == nil || res.Meta.Date == nil || *res.Meta.Date != "2026-11-26" {
		t.Errorf("meta = %+v", res.Meta)
	}
}

func TestEnvKeyAndBaseURL(t *testing.T) {
	var auth, path string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		auth, path = r.Header.Get("Authorization"), r.URL.Path
		writeJSON(w, 200, map[string]any{"data": []any{}})
	}))
	defer srv.Close()
	t.Setenv(EnvAPIKey, testKey)
	t.Setenv(EnvBaseURL, srv.URL+"//")
	c := NewClient()
	if _, err := c.Sports.List(bg); err != nil {
		t.Fatal(err)
	}
	if auth != "Bearer "+testKey || path != "/v2/sports" {
		t.Errorf("auth %q path %q", auth, path)
	}
	// An option wins over the environment.
	c = NewClient(WithAPIKey("other-test-key"))
	if _, err := c.Sports.List(bg); err != nil || auth != "Bearer other-test-key" {
		t.Errorf("option key not used: %v %q", err, auth)
	}
}

func TestMissingKey(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { hits.Add(1) }))
	defer srv.Close()
	t.Setenv(EnvAPIKey, "")
	c := NewClient(WithBaseURL(srv.URL))
	if _, err := c.Sports.List(bg); !errors.Is(err, ErrNoAPIKey) {
		t.Errorf("err = %v, want ErrNoAPIKey", err)
	}
	if _, err := c.Stream(bg, []string{"livescores"}, nil); !errors.Is(err, ErrNoAPIKey) {
		t.Errorf("stream err = %v, want ErrNoAPIKey", err)
	}
	for range c.Teams.FixturesPages(bg, 1, nil) {
	}
	if hits.Load() != 0 {
		t.Errorf("%d requests went out without a key", hits.Load())
	}
}

func TestBaseURLRules(t *testing.T) {
	cases := []struct {
		url string
		ok  bool
	}{
		{"https://api.example.com", true},
		{"https://api.example.com/", true},
		{"http://localhost:8080", true},
		{"http://127.0.0.1:9", true},
		{"http://[::1]:9", true},
		{"http://api.example.com", false},
		{"http://localhost.example.com", false},
		{"ftp://api.example.com", false},
		{"https://user:pw@api.example.com", false},
		{"api.example.com", false},
	}
	for _, tc := range cases {
		c := NewClient(WithAPIKey(testKey), WithBaseURL(tc.url))
		if (c.initErr == nil) != tc.ok {
			t.Errorf("%s: initErr = %v, want ok=%v", tc.url, c.initErr, tc.ok)
		}
		if !tc.ok {
			if _, err := c.Sports.List(bg); !errors.Is(err, ErrInsecureBaseURL) {
				t.Errorf("%s: call err = %v", tc.url, err)
			}
		}
	}
	if c := NewClient(WithAPIKey(testKey), WithBaseURL("https://api.example.com///")); c.baseURL != "https://api.example.com" {
		t.Errorf("trailing slashes kept: %q", c.baseURL)
	}
	t.Setenv(EnvBaseURL, "")
	if c := NewClient(WithAPIKey(testKey)); c.baseURL != DefaultBaseURL || c.initErr != nil {
		t.Errorf("default base = %q, %v", c.baseURL, c.initErr)
	}
}

func TestPathEncoding(t *testing.T) {
	var uri string
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		uri = r.RequestURI
		writeJSON(w, 200, map[string]any{"data": nil})
	})
	if _, err := tc.Rankings.Get(bg, "a/b c?d#e"); err != nil {
		t.Fatal(err)
	}
	if uri != "/v2/rankings/a%2Fb%20c%3Fd%23e" {
		t.Errorf("request URI = %q", uri)
	}
	if _, err := tc.Fixtures.Heatmap(bg, 211521, 42); err != nil {
		t.Fatal(err)
	}
	if uri != "/v2/fixtures/211521/heatmap/42" {
		t.Errorf("request URI = %q", uri)
	}
	if _, err := tc.Leagues.Standings(bg, 17, &LeaguesStandingsParams{Season: 61627, Type: Ptr("home & away")}); err != nil {
		t.Fatal(err)
	}
	if uri != "/v2/leagues/17/standings?season=61627&type=home+%26+away" {
		t.Errorf("request URI = %q", uri)
	}
}

func TestErrorsAndSentinels(t *testing.T) {
	all := []error{ErrBadRequest, ErrUnauthorized, ErrForbidden, ErrNotFound, ErrTooManyRequests, ErrInternal, ErrUnavailable}
	statuses := []int{400, 401, 403, 404, 429, 500, 503}
	for i, status := range statuses {
		tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Retry-After", "7")
			writeJSON(w, status, map[string]any{"error": map[string]any{"code": "some_code", "message": "explained", "upgradeUrl": "https://datasportsapi.com/pricing/"}})
		}, WithMaxRetries(0))
		_, err := tc.Sports.List(bg)
		for j, s := range all {
			if errors.Is(err, s) != (i == j) {
				t.Errorf("status %d: errors.Is(err, %v) = %v", status, s, i != j)
			}
		}
		var apiErr *APIError
		if !errors.As(err, &apiErr) {
			t.Fatalf("status %d: %T is not *APIError", status, err)
		}
		if apiErr.Status != status || apiErr.Code != "some_code" || apiErr.Message != "explained" || apiErr.RetryAfter != 7 || apiErr.UpgradeURL != "https://datasportsapi.com/pricing/" {
			t.Errorf("status %d: %+v", status, apiErr)
		}
		if strings.Contains(err.Error(), testKey) {
			t.Error("the key is in an error message")
		}
	}
	// A status outside the table, with a body that is not the API's.
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(418)
		_, _ = io.WriteString(w, "<html>teapot</html>")
	})
	_, err := tc.Sports.List(bg)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 418 || apiErr.Code != "http_418" || apiErr.RetryAfter != 0 {
		t.Fatalf("err = %#v", err)
	}
	for _, s := range all {
		if errors.Is(err, s) {
			t.Errorf("418 matched %v", s)
		}
	}
}

func TestRetryRateLimitedHonoursRetryAfter(t *testing.T) {
	var hits atomic.Int32
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "3")
			writeJSON(w, 429, apiErrBody("rate_limited", "slow down"))
			return
		}
		writeJSON(w, 200, map[string]any{"data": []any{}})
	})
	if _, err := tc.Sports.List(bg); err != nil {
		t.Fatal(err)
	}
	if hits.Load() != 2 {
		t.Errorf("%d attempts, want 2", hits.Load())
	}
	if w := tc.recordedWaits(); len(w) != 1 || w[0] != 3*time.Second {
		t.Errorf("waits = %v, want [3s]", w)
	}
}

func TestRetryRealSleep(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 1 {
			w.Header().Set("Retry-After", "0.05")
			writeJSON(w, 503, apiErrBody("unavailable", "deploy"))
			return
		}
		writeJSON(w, 200, map[string]any{"data": []any{}})
	}))
	defer srv.Close()
	c := NewClient(WithAPIKey(testKey), WithBaseURL(srv.URL))
	start := time.Now()
	if _, err := c.Sports.List(bg); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 50*time.Millisecond || hits.Load() != 2 {
		t.Errorf("took %s over %d attempts", d, hits.Load())
	}
}

func TestNoRetryOnQuotaExceeded(t *testing.T) {
	var hits atomic.Int32
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "1")
		writeJSON(w, 429, apiErrBody("quota_exceeded", "daily quota used"))
	})
	_, err := tc.Sports.List(bg)
	if !errors.Is(err, ErrTooManyRequests) || hits.Load() != 1 || len(tc.recordedWaits()) != 0 {
		t.Errorf("err %v after %d attempts", err, hits.Load())
	}
}

func TestNoRetryOnOtherStatuses(t *testing.T) {
	var hits atomic.Int32
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		writeJSON(w, 500, apiErrBody("internal", "boom"))
	})
	if _, err := tc.Sports.List(bg); !errors.Is(err, ErrInternal) || hits.Load() != 1 {
		t.Errorf("err %v after %d attempts", err, hits.Load())
	}
}

func TestMaxRetriesAndBackoff(t *testing.T) {
	cases := []struct {
		retries int
		waits   []time.Duration
	}{
		{2, []time.Duration{time.Second, 2 * time.Second}},
		{0, nil},
		{5, []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 8 * time.Second}},
	}
	for _, c := range cases {
		var hits atomic.Int32
		tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
			hits.Add(1)
			writeJSON(w, 503, apiErrBody("unavailable", "busy"))
		}, WithMaxRetries(c.retries))
		_, err := tc.Sports.List(bg)
		if !errors.Is(err, ErrUnavailable) {
			t.Errorf("retries %d: err = %v", c.retries, err)
		}
		if int(hits.Load()) != c.retries+1 {
			t.Errorf("retries %d: %d attempts", c.retries, hits.Load())
		}
		if w := tc.recordedWaits(); fmt.Sprint(w) != fmt.Sprint(c.waits) {
			t.Errorf("retries %d: waits %v, want %v", c.retries, w, c.waits)
		}
	}
}

func TestRetryAfterOverSixtyNotWaited(t *testing.T) {
	var hits atomic.Int32
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.Header().Set("Retry-After", "120")
		writeJSON(w, 429, apiErrBody("rate_limited", "slow down"))
	})
	_, err := tc.Sports.List(bg)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.RetryAfter != 120 || hits.Load() != 1 || len(tc.recordedWaits()) != 0 {
		t.Errorf("err %v, %d attempts, waits %v", err, hits.Load(), tc.recordedWaits())
	}
}

func TestRedirectNotFollowed(t *testing.T) {
	var targetHits atomic.Int32
	var leaked atomic.Value
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		targetHits.Add(1)
		leaked.Store(r.Header.Get("Authorization"))
		writeJSON(w, 200, map[string]any{"data": []any{}})
	}))
	defer target.Close()
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL+"/v2/sports", http.StatusFound)
	})
	_, err := tc.Sports.List(bg)
	var apiErr *APIError
	if !errors.As(err, &apiErr) || apiErr.Status != 302 || apiErr.Code != "http_302" {
		t.Fatalf("err = %v", err)
	}
	if targetHits.Load() != 0 {
		t.Errorf("the redirect was followed (Authorization %q)", leaked.Load())
	}
	// A caller's own http.Client does not change that.
	hc := &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return nil }}
	c := NewClient(WithAPIKey(testKey), WithBaseURL(tc.srv.URL), WithHTTPClient(hc))
	if _, err := c.Sports.List(bg); !errors.As(err, &apiErr) || apiErr.Status != 302 || targetHits.Load() != 0 {
		t.Errorf("custom http.Client followed the redirect: %v", err)
	}
}

func TestKeyRedacted(t *testing.T) {
	c := NewClient(WithAPIKey(testKey), WithBaseURL("https://api.example.com"))
	type holder struct {
		C   *Client
		V   Client
		Key *secretKey
		v   Client
		k   *secretKey
	}
	h := holder{C: c, V: *c, Key: c.key, v: *c, k: c.key}
	for _, s := range []string{
		fmt.Sprintf("%v", c), fmt.Sprintf("%+v", c), fmt.Sprintf("%#v", c), fmt.Sprintf("%s", c), fmt.Sprint(c),
		fmt.Sprintf("%v", *c), fmt.Sprintf("%+v", *c), fmt.Sprintf("%#v", *c),
		fmt.Sprintf("%v", h), fmt.Sprintf("%+v", h), fmt.Sprintf("%#v", h), fmt.Sprintf("%q", c.key), fmt.Sprintf("%x", c.key), fmt.Sprintf("%v", *c.key), fmt.Sprintf("%s", *c.key),
		fmt.Sprintf("%+v", c.Fixtures),
	} {
		if strings.Contains(s, testKey) || strings.Contains(s, fmt.Sprintf("%x", testKey)) {
			t.Errorf("the key shows in %q", s)
		}
	}
	if !strings.Contains(fmt.Sprintf("%+v", c), "[redacted]") {
		t.Errorf("%%+v = %q", fmt.Sprintf("%+v", c))
	}
}

func TestPagingByPage(t *testing.T) {
	var pages []string
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v2/teams/42/fixtures" || r.URL.Query().Get("type") != "recent" {
			t.Errorf("request %s", r.URL)
		}
		p := r.URL.Query().Get("page")
		pages = append(pages, p)
		n := map[string]int{"": 0, "1": 1, "2": 2}[p]
		writeJSON(w, 200, map[string]any{"data": []any{map[string]any{"id": n + 1}}, "meta": map[string]any{"page": n, "hasNextPage": n < 2}})
	})
	var got int
	for page, err := range tc.Teams.FixturesPages(bg, 42, &TeamsFixturesParams{Type: Ptr("recent")}) {
		if err != nil {
			t.Fatal(err)
		}
		got += len(page.Data)
	}
	if got != 3 || fmt.Sprint(pages) != "[ 1 2]" {
		t.Errorf("got %d items over pages %q", got, pages)
	}

	pages = nil
	n := 0
	for _, err := range tc.Teams.FixturesPages(bg, 42, &TeamsFixturesParams{Type: Ptr("recent")}, MaxPages(2)) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 2 || len(pages) != 2 {
		t.Errorf("MaxPages(2): %d pages, %d requests", n, len(pages))
	}

	pages = nil
	for range tc.Teams.FixturesPages(bg, 42, &TeamsFixturesParams{Type: Ptr("recent")}) {
		break
	}
	if len(pages) != 1 {
		t.Errorf("break: %d requests", len(pages))
	}

	// The caller's params are not changed by the walk.
	params := &TeamsFixturesParams{Type: Ptr("recent"), Page: Ptr(int64(1))}
	pages = nil
	for _, err := range tc.Teams.FixturesPages(bg, 42, params) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if *params.Page != 1 || fmt.Sprint(pages) != "[1 2]" {
		t.Errorf("params.Page = %d, pages %q", *params.Page, pages)
	}
}

func TestPagingWithoutMetaPage(t *testing.T) {
	var pages []string
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Query().Get("page")
		pages = append(pages, p)
		writeJSON(w, 200, map[string]any{"data": []any{}, "meta": map[string]any{"hasNextPage": p != "6"}})
	})
	for _, err := range tc.Referees.FixturesPages(bg, 7, &RefereesFixturesParams{Page: Ptr(int64(5))}) {
		if err != nil {
			t.Fatal(err)
		}
	}
	if fmt.Sprint(pages) != "[5 6]" {
		t.Errorf("pages %q", pages)
	}
}

func TestPagingByCursor(t *testing.T) {
	var queries []string
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		queries = append(queries, r.URL.RawQuery)
		switch q.Get("cursor") {
		case "":
			writeJSON(w, 200, map[string]any{"data": []any{}, "meta": map[string]any{"page": 0, "hasNextPage": true, "nextCursor": "c/1"}})
		case "c/1":
			writeJSON(w, 200, map[string]any{"data": []any{}, "meta": map[string]any{"hasNextPage": false, "nextCursor": nil}})
		}
	})
	n := 0
	for _, err := range tc.Fixtures.ListPages(bg, &FixturesListParams{Date: "2026-11-26", Page: Ptr(int64(0))}) {
		if err != nil {
			t.Fatal(err)
		}
		n++
	}
	if n != 2 || len(queries) != 2 || queries[0] != "date=2026-11-26&page=0" || queries[1] != "cursor=c%2F1&date=2026-11-26" {
		t.Errorf("%d pages, queries %q", n, queries)
	}
}

func TestPagingStopsOnError(t *testing.T) {
	var hits atomic.Int32
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if hits.Add(1) == 2 {
			writeJSON(w, 404, apiErrBody("not_found", "gone"))
			return
		}
		writeJSON(w, 200, map[string]any{"data": []any{}, "meta": map[string]any{"page": 0, "hasNextPage": true}})
	})
	var errs, oks int
	for page, err := range tc.Search.QueryPages(bg, &SearchQueryParams{Q: "arsenal"}) {
		if err != nil {
			if page != nil || !errors.Is(err, ErrNotFound) {
				t.Errorf("page %v err %v", page, err)
			}
			errs++
			continue
		}
		oks++
	}
	if oks != 1 || errs != 1 || hits.Load() != 2 {
		t.Errorf("%d pages, %d errors, %d requests", oks, errs, hits.Load())
	}
}

func TestExportsCreateBody(t *testing.T) {
	var method, ctype string
	var body map[string]any
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		method, ctype = r.Method, r.Header.Get("Content-Type")
		body = nil
		_ = json.NewDecoder(r.Body).Decode(&body)
		writeJSON(w, 202, map[string]any{"data": map[string]any{"id": 9, "status": "queued"}})
	})
	res, err := tc.Exports.Create(bg, &ExportsCreateParams{League: 17, Season: 61627, Format: Ptr("csv")})
	if err != nil {
		t.Fatal(err)
	}
	if method != "POST" || ctype != "application/json" {
		t.Errorf("%s with Content-Type %q", method, ctype)
	}
	if fmt.Sprint(body) != "map[format:csv league:17 season:61627]" {
		t.Errorf("body = %v", body)
	}
	if res.Data.ID != 9 {
		t.Errorf("data = %+v", res.Data)
	}
	if _, err := tc.Exports.Create(bg, &ExportsCreateParams{League: 17, Season: 61627}); err != nil {
		t.Fatal(err)
	}
	if _, ok := body["format"]; ok {
		t.Errorf("an unset optional field was sent: %v", body)
	}
}

func TestTimeoutAndNetworkErrors(t *testing.T) {
	release := make(chan struct{})
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-release:
		}
	}, WithTimeout(50*time.Millisecond))
	defer close(release)
	_, err := tc.Sports.List(bg)
	var apiErr *APIError
	if err == nil || errors.As(err, &apiErr) || !errors.Is(err, context.DeadlineExceeded) {
		t.Errorf("timeout err = %v", err)
	}

	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	base := srv.URL
	srv.Close()
	c := NewClient(WithAPIKey(testKey), WithBaseURL(base))
	if _, err := c.Sports.List(bg); err == nil || errors.As(err, &apiErr) || strings.Contains(err.Error(), testKey) {
		t.Errorf("network err = %v", err)
	}
}

func TestDecodeErrorIsReported(t *testing.T) {
	tc := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(w, `{"data": [{"id": "not a number"}]}`)
	})
	if _, err := tc.Livescores.List(bg, nil); err == nil || !strings.Contains(err.Error(), "decoding") {
		t.Errorf("err = %v", err)
	}
}
