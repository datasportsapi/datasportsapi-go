//go:build live

package datasportsapi

// A smoke test against the real API: it walks real ids and checks that every
// answer decodes into the generated types. It runs only with the build tag and a
// key, never in CI:
//
//	DATASPORTSAPI_API_KEY=… go test -tags live -run Live -v ./...
//
// 403 (not in the plan) and 404 (no data for this match) answers are logged, not
// failed; anything else that is not a clean decode fails the test.

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"
)

func liveClient(t *testing.T) *Client {
	t.Helper()
	if os.Getenv(EnvAPIKey) == "" {
		t.Skip("set " + EnvAPIKey + " to run the live smoke test")
	}
	return NewClient()
}

// liveCheck fails on anything but success, a 403 or a 404.
func liveCheck(t *testing.T, what string, err error) bool {
	t.Helper()
	if err == nil {
		return true
	}
	if errors.Is(err, ErrForbidden) || errors.Is(err, ErrNotFound) {
		t.Logf("%s: %v (skipped)", what, err)
		return false
	}
	t.Errorf("%s: %v", what, err)
	return false
}

func TestLiveWalk(t *testing.T) {
	c := liveClient(t)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	sports, err := c.Sports.List(ctx)
	if liveCheck(t, "sports.list", err) && len(sports.Data) == 0 {
		t.Error("sports.list: no sports")
	}

	// A fixture: one in play, else one from today's football list.
	var fixture *Fixture
	if live, err := c.Livescores.List(ctx, &LivescoresListParams{Sport: Ptr("football")}); liveCheck(t, "livescores.list", err) && len(live.Data) > 0 {
		fixture = &live.Data[0]
	}
	if fixture == nil {
		today := time.Now().UTC().Format("2006-01-02")
		list, err := c.Fixtures.List(ctx, &FixturesListParams{Date: today, Sport: Ptr("football")})
		if !liveCheck(t, "fixtures.list", err) || len(list.Data) == 0 {
			t.Fatal("no fixture to walk")
		}
		fixture = &list.Data[0]
	}
	id := fixture.ID
	t.Logf("walking fixture %d (%s)", id, fixture.Sport)

	_, err = c.Fixtures.Get(ctx, id)
	liveCheck(t, "fixtures.get", err)
	_, err = c.Fixtures.Lineups(ctx, id)
	liveCheck(t, "fixtures.lineups", err)
	_, err = c.Fixtures.Statistics(ctx, id)
	liveCheck(t, "fixtures.statistics", err)
	_, err = c.Fixtures.Events(ctx, id)
	liveCheck(t, "fixtures.events", err)
	_, err = c.Fixtures.H2h(ctx, id)
	liveCheck(t, "fixtures.h2h", err)

	if fixture.Home != nil {
		_, err = c.Teams.Get(ctx, fixture.Home.ID)
		liveCheck(t, "teams.get", err)
		squad, err := c.Teams.Squad(ctx, fixture.Home.ID)
		if liveCheck(t, "teams.squad", err) {
			_ = squad
		}
		pages := 0
		for _, err := range c.Teams.FixturesPages(ctx, fixture.Home.ID, &TeamsFixturesParams{Type: Ptr("recent")}, MaxPages(2)) {
			if !liveCheck(t, "teams.fixturesPages", err) {
				break
			}
			pages++
		}
		t.Logf("teams.fixturesPages: %d pages", pages)
	}
	if fixture.Away != nil {
		_, err = c.Teams.Get(ctx, fixture.Away.ID)
		liveCheck(t, "teams.get (away)", err)
	}

	if fixture.League != nil {
		_, err = c.Leagues.Get(ctx, fixture.League.ID)
		liveCheck(t, "leagues.get", err)
		_, err = c.Leagues.Seasons(ctx, fixture.League.ID)
		liveCheck(t, "leagues.seasons", err)
		if fixture.Season != nil {
			_, err = c.Leagues.Standings(ctx, fixture.League.ID, &LeaguesStandingsParams{Season: fixture.Season.ID})
			liveCheck(t, "leagues.standings", err)
		}
	}

	found, err := c.Search.Query(ctx, &SearchQueryParams{Q: "arsenal"})
	if liveCheck(t, "search.query", err) && len(found.Data) == 0 {
		t.Log("search.query: no results")
	}
	pages := 0
	for _, err := range c.Search.QueryPages(ctx, &SearchQueryParams{Q: "united"}, MaxPages(2)) {
		if !liveCheck(t, "search.queryPages", err) {
			break
		}
		pages++
	}
	t.Logf("search.queryPages: %d pages", pages)
}
