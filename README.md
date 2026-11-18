# datasportsapi-go

The Go client for [DataSportsAPI](https://datasportsapi.com), a real-time sports
data API for 24 sports.

> **In development.** The Go module arrives with the 0.1 SDKs, generated from the
> [OpenAPI description](https://github.com/datasportsapi/openapi) like the
> TypeScript and Python clients. Watch this repository to hear when it lands.

The API itself is live today. Until the module is out, the standard library is
enough:

```go
req, _ := http.NewRequest("GET", "https://api.datasportsapi.com/v2/sports", nil)
req.Header.Set("Authorization", "Bearer "+os.Getenv("DATASPORTSAPI_API_KEY"))
res, err := http.DefaultClient.Do(req)
```

Every answer is a `{ "data": ..., "meta": ... }` envelope; errors carry a code,
a message and, on 429, `Retry-After`. Get a free key (1,000 requests a day, no
card) at [datasportsapi.com/signup](https://datasportsapi.com/signup/), and read
the [docs](https://datasportsapi.com/docs/).

Questions: [hello@datasportsapi.com](mailto:hello@datasportsapi.com) or the [developer Discord](https://datasportsapi.com/discord).
© Noventra Software Ltd. MIT licensed.
