package datasportsapi_test

// The code samples of https://datasportsapi.com/sdks/go/ (src/data/sdks.ts →
// golang), kept as Example functions so `go test` compiles them: a sample that no
// longer matches the package fails the build. None of them has an Output comment,
// so none of them runs (they would need a key and the network).

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"

	"github.com/datasportsapi/datasportsapi-go"
)

var ctx = context.Background()

// The /sdks/ card: the first call.
func Example() {
	client := datasportsapi.NewClient() // reads DATASPORTSAPI_API_KEY
	res, err := client.Livescores.List(ctx, &datasportsapi.LivescoresListParams{Sport: datasportsapi.Ptr("football")})
	_, _ = res, err
}

// Pass the key.
func ExampleWithAPIKey() {
	client := datasportsapi.NewClient(datasportsapi.WithAPIKey("sk_live_…")) // or DATASPORTSAPI_API_KEY
	_ = client
}

// Make a request.
func ExampleLivescoresService_List() {
	client := datasportsapi.NewClient()
	live, err := client.Livescores.List(ctx, &datasportsapi.LivescoresListParams{Sport: datasportsapi.Ptr("football")})
	if err != nil {
		log.Fatal(err)
	}
	for _, f := range live.Data {
		fmt.Println(f.Home.Name, *f.HomeScore.Current, "-", *f.AwayScore.Current, f.Away.Name)
	}

	table, err := client.Leagues.Standings(ctx, 17, &datasportsapi.LeaguesStandingsParams{Season: 61627})
	_, _ = table, err
}

// Errors.
func ExampleAPIError() {
	client := datasportsapi.NewClient()
	_, err := client.Fixtures.Momentum(ctx, 211521)
	switch {
	case errors.Is(err, datasportsapi.ErrForbidden): // 403: endpoint_not_in_plan, …
	case errors.Is(err, datasportsapi.ErrNotFound): // 404: not_found, no_data, not_supported
	}
	var apiErr *datasportsapi.APIError
	if errors.As(err, &apiErr) {
		fmt.Println(apiErr.Status, apiErr.Code, apiErr.RetryAfter)
	}
}

// Retries and settings.
func ExampleWithMaxRetries() {
	key := os.Getenv("DATASPORTSAPI_API_KEY")
	client := datasportsapi.NewClient(
		datasportsapi.WithAPIKey(key), // or DATASPORTSAPI_API_KEY
		datasportsapi.WithMaxRetries(2),
	)
	// DATASPORTSAPI_BASE_URL changes the base URL
	_ = client
}

// Paging.
func ExampleTeamsService_FixturesPages() {
	client := datasportsapi.NewClient()
	for page, err := range client.Teams.FixturesPages(ctx, 42, &datasportsapi.TeamsFixturesParams{Type: datasportsapi.Ptr("recent")}) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(len(page.Data))
	}
}

// Paging with a cap.
func ExampleMaxPages() {
	client := datasportsapi.NewClient()
	for page, err := range client.Search.QueryPages(ctx, &datasportsapi.SearchQueryParams{Q: "arsenal"}, datasportsapi.MaxPages(3)) {
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(len(page.Data))
	}
}

// Live stream (Business and up).
func ExampleClient_Stream() {
	client := datasportsapi.NewClient()
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
}
