// Package datasportsapi is the Go client for DataSportsAPI, a real-time sports data
// API (https://datasportsapi.com).
//
// Every endpoint is a method on a service of the Client (client.Fixtures.Get,
// client.Teams.Fixtures, …). Each call takes a context.Context, returns the API's
// { data, meta } envelope as a typed struct, and fails with an error that matches one
// of the sentinels (ErrForbidden, ErrNotFound, …) through errors.Is, or an *APIError
// through errors.As. Paged lists have a range-over-func iterator (…Pages), and
// Client.Stream keeps a live copy of the fixtures on WebSocket channels.
//
//	client := datasportsapi.NewClient() // reads DATASPORTSAPI_API_KEY
//	live, err := client.Livescores.List(ctx, &datasportsapi.LivescoresListParams{Sport: datasportsapi.Ptr("football")})
//
// Docs: https://datasportsapi.com/sdks/go/
package datasportsapi

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

// Version is the version of this module.
const Version = "0.1.0"

const (
	// DefaultBaseURL is where the client sends requests unless told otherwise.
	DefaultBaseURL = "https://api.datasportsapi.com"
	// EnvAPIKey is the environment variable the client reads its key from.
	EnvAPIKey = "DATASPORTSAPI_API_KEY"
	// EnvBaseURL is the environment variable that overrides the base URL.
	EnvBaseURL = "DATASPORTSAPI_BASE_URL"

	defaultTimeout    = 30 * time.Second
	defaultMaxRetries = 2
	userAgent         = "datasportsapi-go/" + Version
	maxRetryAfter     = 60 * time.Second
	maxBackoff        = 8 * time.Second
)

// secretKey holds the API key. Every way fmt can print it shows "[redacted]".
type secretKey string

func (secretKey) String() string   { return "[redacted]" }
func (secretKey) GoString() string { return "[redacted]" }

// Format prints "[redacted]" for every verb, %v, %+v, %#v, %s and %q included.
func (secretKey) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, "[redacted]") }

// Client talks to the API. Create one with NewClient and share it: it is safe for
// concurrent use. Its services (Fixtures, Teams, …) hold one method per endpoint.
type Client struct {
	services

	// key sits behind a pointer so that even fmt's raw printing of unexported
	// fields (a Client inside another struct) shows an address, never the key.
	key        *secretKey
	baseURL    string
	timeout    time.Duration
	maxRetries int
	http       *http.Client
	// initErr is the configuration error every call returns (no key, insecure base URL).
	initErr error

	// Test hooks: how the client waits between attempts, and the stream's settings.
	sleep         func(ctx context.Context, d time.Duration) error
	streamBackoff time.Duration
	wsMaxFrame    int64

	// Set by options before NewClient settles the configuration.
	optKey     *string
	optBaseURL *string
}

// Option configures a Client.
type Option func(*Client)

// WithAPIKey sets the API key. Without it the client reads DATASPORTSAPI_API_KEY.
func WithAPIKey(key string) Option {
	return func(c *Client) { c.optKey = &key }
}

// WithBaseURL points the client at another base URL. It must be https://, except
// for localhost, 127.0.0.1 and [::1]. Without it the client reads
// DATASPORTSAPI_BASE_URL, else uses DefaultBaseURL.
func WithBaseURL(u string) Option {
	return func(c *Client) { c.optBaseURL = &u }
}

// WithTimeout sets the time limit of each attempt (default 30 s).
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if d > 0 {
			c.timeout = d
		}
	}
}

// WithMaxRetries sets how many more attempts a 429 rate_limited or a 503 gets
// (default 2; 0 turns retries off).
func WithMaxRetries(n int) Option {
	return func(c *Client) {
		if n >= 0 {
			c.maxRetries = n
		}
	}
}

// WithHTTPClient makes the client send its requests through hc (a copy of it, whose
// CheckRedirect is replaced: redirects are never followed). Its transport's TLS
// settings are used for the stream too.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) {
		if hc != nil {
			cp := *hc
			c.http = &cp
		}
	}
}

// NewClient returns a client configured by opts and the environment. It never
// fails: a missing key or an unusable base URL makes every call return the error
// (ErrNoAPIKey, ErrInsecureBaseURL) instead.
func NewClient(opts ...Option) *Client {
	c := &Client{
		timeout:    defaultTimeout,
		maxRetries: defaultMaxRetries,
		sleep:      sleepCtx,
		http:       &http.Client{},
	}
	for _, o := range opts {
		if o != nil {
			o(c)
		}
	}
	c.http.CheckRedirect = func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }

	key := os.Getenv(EnvAPIKey)
	if c.optKey != nil {
		key = *c.optKey
	}
	k := secretKey(strings.TrimSpace(key))
	c.key = &k
	c.optKey = nil

	base := os.Getenv(EnvBaseURL)
	if c.optBaseURL != nil {
		base = *c.optBaseURL
	}
	c.optBaseURL = nil
	if strings.TrimSpace(base) == "" {
		base = DefaultBaseURL
	}
	base = strings.TrimRight(strings.TrimSpace(base), "/")
	c.baseURL = base

	if err := checkBaseURL(base); err != nil {
		c.initErr = err
	} else if *c.key == "" {
		c.initErr = ErrNoAPIKey
	}
	c.initServices()
	return c
}

// String describes the client without its key.
func (c Client) String() string {
	return fmt.Sprintf("datasportsapi.Client{baseURL: %q, key: [redacted], timeout: %s, maxRetries: %d}", c.baseURL, c.timeout, c.maxRetries)
}

// GoString is String: %#v never shows the key either.
func (c Client) GoString() string { return c.String() }

// Format prints String for every verb, so %v, %+v and %#v never show the key,
// whether the Client is printed as a value or through a pointer.
func (c Client) Format(f fmt.State, _ rune) { _, _ = io.WriteString(f, c.String()) }

// isLocalHost reports whether host is one of the hosts allowed without TLS.
func isLocalHost(host string) bool {
	switch strings.ToLower(host) {
	case "localhost", "127.0.0.1", "::1":
		return true
	}
	return false
}

func checkBaseURL(base string) error {
	u, err := url.Parse(base)
	if err != nil || u.Host == "" {
		return fmt.Errorf("%w: %q is not an absolute URL", ErrInsecureBaseURL, base)
	}
	if u.User != nil || u.RawQuery != "" || u.Fragment != "" {
		return fmt.Errorf("%w: the base URL may not carry credentials, a query or a fragment", ErrInsecureBaseURL)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if isLocalHost(u.Hostname()) {
			return nil
		}
	}
	return fmt.Errorf("%w: %s://%s is not https", ErrInsecureBaseURL, u.Scheme, u.Host)
}

func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// do sends one API call, retrying 429 rate_limited and 503 answers, and decodes a
// 2xx answer into out.
func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	if c.initErr != nil {
		return c.initErr
	}
	if ctx == nil {
		ctx = context.Background()
	}
	var payload []byte
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("datasportsapi: encoding the request body: %w", err)
		}
		payload = b
	}
	target := c.baseURL + path
	if len(query) > 0 {
		target += "?" + query.Encode()
	}
	for attempt := 0; ; attempt++ {
		err := c.once(ctx, method, target, payload, out)
		if err == nil {
			return nil
		}
		var apiErr *APIError
		if !errors.As(err, &apiErr) || !apiErr.retryable() || attempt >= c.maxRetries {
			return err
		}
		wait := time.Duration(math.Min(float64(time.Second)*math.Pow(2, float64(attempt)), float64(maxBackoff)))
		if apiErr.retryAfter != nil {
			if *apiErr.retryAfter > maxRetryAfter {
				return err
			}
			wait = *apiErr.retryAfter
		}
		if serr := c.sleep(ctx, wait); serr != nil {
			return fmt.Errorf("datasportsapi: waiting to retry: %w", serr)
		}
	}
}

func (c *Client) once(ctx context.Context, method, target string, payload []byte, out any) error {
	actx, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	var rd io.Reader
	if payload != nil {
		rd = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(actx, method, target, rd)
	if err != nil {
		return fmt.Errorf("datasportsapi: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+string(*c.key))
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", userAgent)
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return fmt.Errorf("datasportsapi: %w", err)
	}
	defer res.Body.Close()
	data, err := io.ReadAll(res.Body)
	if err != nil {
		return fmt.Errorf("datasportsapi: reading the answer: %w", err)
	}
	if res.StatusCode < 200 || res.StatusCode > 299 {
		return newAPIError(res, data)
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(data, out); err != nil {
		return fmt.Errorf("datasportsapi: decoding the answer of %s %s: %w", method, req.URL.Path, err)
	}
	return nil
}

// parseRetryAfter reads a numeric Retry-After header (seconds).
func parseRetryAfter(h string) (time.Duration, bool) {
	h = strings.TrimSpace(h)
	if h == "" {
		return 0, false
	}
	f, err := strconv.ParseFloat(h, 64)
	if err != nil || f < 0 || math.IsNaN(f) || math.IsInf(f, 0) {
		return 0, false
	}
	return time.Duration(f * float64(time.Second)), true
}
