package batch

import (
	"fmt"
	"testing"
	"time"

	"trending-songs/internal/catalog"
	"trending-songs/internal/domain"
	"trending-songs/internal/olap"
	"trending-songs/internal/playcount"
)

var hour = time.Date(2026, 10, 27, 21, 0, 0, 0, time.UTC)

func listen(id, user, track string, start time.Time, d time.Duration) []domain.Event {
	var out []domain.Event
	for at := time.Duration(0); at <= d; at += 5 * time.Second {
		st := domain.StateHeartbeat
		if at == 0 {
			st = domain.StatePlay
		}
		out = append(out, domain.Event{EventID: fmt.Sprintf("%s-%d", id, len(out)), SessionID: id, UserID: user, TrackID: track,
			Country: "BR", PositionMs: at.Milliseconds(), State: st, ClientTs: start.Add(at)})
	}
	return out
}

func TestRecomputeDedupesAppliesAbuseRuleAndReconciles(t *testing.T) {
	c := catalog.Generate(10, 1)
	lake := NewLake()
	var facts []domain.PlayFact
	add := func(events []domain.Event) {
		// 資料湖保留原始事件，連重送的重複事件也在。
		lake.Append(append(events, events...))
		s := playcount.NewSessionizer(playcount.DefaultRules())
		for _, e := range events {
			if f, ok := s.Process(e); ok {
				meta, _ := c.Get(f.TrackID)
				f.Genre = meta.Genre
				facts = append(facts, f)
			}
		}
	}
	// 機器人一小時內把同一首歌聽了 7 次，每次剛好過門檻。
	for i := range 7 {
		add(listen(fmt.Sprintf("bot-%d", i), "bot", "trk-00001", hour.Add(time.Duration(i)*time.Minute), 35*time.Second))
	}
	add(listen("fan", "fan", "trk-00002", hour.Add(10*time.Minute), 3*time.Minute))
	// 跨過整點的播放算在達到門檻的那一小時。
	add(listen("edge", "fan", "trk-00003", hour.Add(-20*time.Second), time.Minute))
	add(listen("next", "fan", "trk-00003", hour.Add(time.Hour), time.Minute))

	r := NewJob(lake, c, playcount.DefaultRules(), 5).Recompute(hour)
	if r.Plays != 9 || r.Removed != 2 {
		t.Fatalf("plays=%d removed=%d", r.Plays, r.Removed)
	}
	want := map[string]int64{"trk-00001": 5, "trk-00002": 1, "trk-00003": 1}
	if !equal(r.Hourly[domain.DimGlobal], want) {
		t.Fatalf("hourly = %v", r.Hourly[domain.DimGlobal])
	}

	store := olap.NewStore(olap.Config{Shards: 4})
	store.Apply(0, 0, facts)
	if streamPlays := Reconcile(store, r); streamPlays != 9 {
		t.Fatalf("stream plays = %d", streamPlays)
	}
	if got := store.Counts("country:BR", olap.Hour, hour, hour.Add(time.Hour)); !equal(got, want) {
		t.Fatalf("after reconcile = %v", got)
	}
	if got := store.Counts(domain.DimGlobal, olap.Hour, hour.Add(time.Hour), hour.Add(2*time.Hour)); !equal(got, map[string]int64{"trk-00003": 1}) {
		t.Fatalf("next hour should be untouched, got %v", got)
	}
}

func equal(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if b[k] != v {
			return false
		}
	}
	return true
}
