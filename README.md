# datasportsapi-go

The official Go client for [DataSportsAPI](https://datasportsapi.com), a real-time
sports data API covering 25 sports. Every endpoint is a typed method that takes a
`context.Context`, paged lists are range-over-func iterators, and the live stream
is a loop over frames. Standard library only; Go 1.23 or newer.

```bash
go get github.com/datasportsapi/datasportsapi-go
```

## Your key

Create a key in your dashboard ([free plan](https://datasportsapi.com/signup/), no
card). The client reads it from `DATASPORTSAPI_API_KEY`, or you hand it over in code:

```go
client := datasportsapi.NewClient() // reads DATASPORTSAPI_API_KEY
client = datasportsapi.NewClient(datasportsapi.WithAPIKey("sk_live_…"))
```

`NewClient` never fails. If there is no key, every call returns
`datasportsapi.ErrNoAPIKey`, so check the first error you get back.

## First calls

Path values come first, then a `*…Params` struct for the query string. Required
fields are plain values; optional ones are pointers, and `datasportsapi.Ptr` makes
one. An operation without query parameters takes no struct at all.

```go
ctx := context.Background()

live, err := client.Livescores.List(ctx, &datasportsapi.LivescoresListParams{Sport: datasportsapi.Ptr("football")})
if err != nil {
	log.Fatal(err)
}
for _, f := range live.Data {
	fmt.Println(f.Home.Name, *f.HomeScore.Current, "-", *f.AwayScore.Current, f.Away.Name)
}

table, err := client.Leagues.Standings(ctx, 17, &datasportsapi.LeaguesStandingsParams{Season: 61627})
sports, err := client.Sports.List(ctx)
```

Each method returns the API's envelope: `Data` holds the answer, `Meta` (when the
endpoint has one) the paging and the filters that were applied. Fields the API
adds later are ignored, so a new field never breaks decoding.

## Errors

Branch on the status with `errors.Is` and on the API's error code with
`errors.As`. Messages are written for people and may change; codes do not.

```go
_, err := client.Fixtures.Momentum(ctx, 211521)
switch {
case errors.Is(err, datasportsapi.ErrForbidden): // 403: endpoint_not_in_plan, …
case errors.Is(err, datasportsapi.ErrNotFound): // 404: not_found, no_data, not_supported
}
var apiErr *datasportsapi.APIError
if errors.As(err, &apiErr) {
	fmt.Println(apiErr.Status, apiErr.Code, apiErr.RetryAfter, apiErr.UpgradeURL)
}
```

| Status | Sentinel |
|---|---|
| 400 | `ErrBadRequest` |
| 401 | `ErrUnauthorized` |
| 403 | `ErrForbidden` |
| 404 | `ErrNotFound` |
| 429 | `ErrTooManyRequests` |
| 500 | `ErrInternal` |
| 503 | `ErrUnavailable` |

Any other status is still an `*APIError`, just without a sentinel. Network
failures and timeouts come back as the wrapped transport error (`errors.Is(err,
context.DeadlineExceeded)` works).

## Retries and options

Two answers are worth a second try: `429` with the code `rate_limited`, and
`503`. The client waits for the `Retry-After` they carry (1 s, 2 s, 4 s … up to
8 s when there is none) and tries again, two more times by default. A
`Retry-After` above 60 seconds is not waited out: you get the error at once. A
`429 quota_exceeded` is never retried, since a daily quota does not refill in
seconds.

```go
client := datasportsapi.NewClient(
	datasportsapi.WithAPIKey(key),
	datasportsapi.WithMaxRetries(4),           // 0 turns retries off
	datasportsapi.WithTimeout(10*time.Second), // per attempt; 30 s by default
	datasportsapi.WithHTTPClient(myHTTPClient),
	datasportsapi.WithBaseURL("https://api.datasportsapi.com"), // or DATASPORTSAPI_BASE_URL
)
```

## Paging

Each paged endpoint has an iterator named after its method plus `Pages`. It asks
for the next page (or follows the cursor, where the endpoint has one) until the
list ends; `MaxPages` stops it earlier, and so does `break`.

```go
for page, err := range client.Teams.FixturesPages(ctx, 42, &datasportsapi.TeamsFixturesParams{Type: datasportsapi.Ptr("recent")}) {
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(len(page.Data))
}

for page, err := range client.Search.QueryPages(ctx, &datasportsapi.SearchQueryParams{Q: "arsenal"}, datasportsapi.MaxPages(3)) {
	// …
}
```

## Live stream (Business plan and up)

`Stream` opens the WebSocket, subscribes to your channels and keeps its own copy
of every fixture on them, with each delta applied as it arrives. Tickets,
sequence numbers, resyncs after a gap and reconnects (with replay of missed
frames where your plan has it) happen inside. A server restart (close code 1012)
is met with an immediate reconnect.

```go
s, err := client.Stream(ctx, []string{"livescores", "fixture:237609"}, nil)
if err != nil {
	log.Fatal(err)
}
defer s.Close()
for f := range s.Frames() {
	if d, ok := f.Value.(*datasportsapi.WsDelta); ok {
		fmt.Println("delta", d.ID)
	}
	live, _ := s.Fixtures("livescores") // the stream's own copy, deltas applied
	_ = live
}
if err := s.Err(); err != nil {
	log.Println("stream ended:", err)
}
```

Every frame the server sends is passed on: `Type`, `Channel`, `Seq`, the raw JSON
and `Value`, the frame decoded into its struct (`*WsSnapshot`, `*WsDelta`,
`*WsOdds`, …). `Subscribe` and `Unsubscribe` change channels on the fly.
`Fixtures` works for `livescores`, `sport:*`, `league:*`, `team:*` and
`fixture:*`; other channels (odds, tracking, races) keep no state, so read their
frames directly. The stream ends on `Close`, when `ctx` ends, or on a fatal close
(1008 without "reconnect" in the reason, 1013, or a refused ticket); `Err` says
which.

## Security

- The key is sent only in the `Authorization` header, and only to the base URL
  you configured. It never appears in a URL, an error message or a log line, and
  printing a `Client` (`%v`, `%+v`, `%#v`) shows `[redacted]`.
- Redirects are never followed: a `3xx` answer is returned as an error, so the
  header cannot travel to another host.
- The base URL must be `https://`; plain `http://` is accepted only for
  `localhost`, `127.0.0.1` and `[::1]`.
- The stream authenticates with a single-use ticket; the key is never part of
  the socket URL.
- Keep secret keys (`sk_live_…`) on servers. This repository's CI scans every
  push for anything shaped like a key.

## Development

The typed layer (`*_gen.go`, `docs_samples_test.go`) is generated from the
OpenAPI description by the DataSportsAPI team; the runtime around it is written
by hand. `go test ./...` runs the offline suite against local HTTP and WebSocket
servers. A smoke test against the real API runs only with a key:

```bash
DATASPORTSAPI_API_KEY=sk_live_… go test -tags live -run Live -v ./...
```

## Links

- Go SDK guide: https://datasportsapi.com/sdks/go/
- API docs: https://datasportsapi.com/docs/
- Questions: [hello@datasportsapi.com](mailto:hello@datasportsapi.com) or the [developer Discord](https://datasportsapi.com/discord) (#go)

© DataSportsAPI. MIT licensed.
