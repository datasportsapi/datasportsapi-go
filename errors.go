package datasportsapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

var (
	// ErrNoAPIKey is returned by every call of a client that has no key: neither
	// WithAPIKey nor DATASPORTSAPI_API_KEY gave one.
	ErrNoAPIKey = errors.New("datasportsapi: no API key: pass WithAPIKey or set " + EnvAPIKey)
	// ErrInsecureBaseURL is returned by every call of a client whose base URL is not
	// https:// (plain http is allowed only for localhost, 127.0.0.1 and [::1]).
	ErrInsecureBaseURL = errors.New("datasportsapi: refusing the base URL")
	// ErrNoState is returned by Stream.Fixtures for a channel that keeps no fixtures.
	ErrNoState = errors.New("datasportsapi: this channel keeps no fixture state")
	// ErrStreamClosed is returned by Stream.Subscribe and Unsubscribe after the stream ended.
	ErrStreamClosed = errors.New("datasportsapi: the stream is closed")
)

// APIError is a non-2xx answer of the API. Compare it with the sentinels through
// errors.Is (ErrForbidden, ErrNotFound, …), or read its fields through errors.As.
type APIError struct {
	// Status is the HTTP status.
	Status int
	// Code is the API's error code (endpoint_not_in_plan, rate_limited, …), or
	// http_<status> when the answer carried none.
	Code string
	// Message explains the error to a person; write logic against Code instead.
	Message string
	// RetryAfter is the Retry-After header in whole seconds (rounded up), 0 when absent.
	RetryAfter int
	// UpgradeURL is where to change plans, when the API sent one.
	UpgradeURL string

	retryAfter *time.Duration
}

func (e *APIError) Error() string {
	msg := e.Message
	if msg == "" {
		msg = http.StatusText(e.Status)
	}
	return fmt.Sprintf("datasportsapi: %d %s: %s", e.Status, e.Code, msg)
}

// Is reports whether target is the sentinel of e's status.
func (e *APIError) Is(target error) bool {
	s, ok := sentinelByStatus[e.Status]
	return ok && s == target
}

func (e *APIError) retryable() bool {
	return e.Status == http.StatusServiceUnavailable || (e.Status == http.StatusTooManyRequests && e.Code == "rate_limited")
}

func newAPIError(res *http.Response, body []byte) *APIError {
	e := &APIError{Status: res.StatusCode}
	var env struct {
		Error *struct {
			Code       string `json:"code"`
			Message    string `json:"message"`
			UpgradeURL string `json:"upgradeUrl"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &env) == nil && env.Error != nil {
		e.Code = env.Error.Code
		e.Message = env.Error.Message
		e.UpgradeURL = env.Error.UpgradeURL
	}
	if e.Code == "" {
		e.Code = "http_" + strconv.Itoa(res.StatusCode)
	}
	if e.Message == "" {
		e.Message = http.StatusText(res.StatusCode)
		if res.StatusCode >= 300 && res.StatusCode < 400 {
			e.Message = strings.TrimSpace(e.Message + "; redirects are not followed")
		}
	}
	if d, ok := parseRetryAfter(res.Header.Get("Retry-After")); ok {
		e.retryAfter = &d
		e.RetryAfter = int(math.Ceil(d.Seconds()))
	}
	return e
}
