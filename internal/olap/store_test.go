package olap

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"
	"time"

	"trending-songs/internal/domain"
)

var t0 = time.Date(2026, 10, 27, 21, 0, 0, 0, time.UTC)

func fact(i int, track string, at time.Time) domain.PlayFact {
	return domain.PlayFact{PlayID: fmt.Sprintf("p%d", i), UserID: "u1", TrackID: track, Country: "US", Genre: "pop", At: at}
}

func TestReplayedOffsetsAreAppliedOnce(t *testing.T) {
	s := NewStore(Config{Shards: 4, Hot: map[string]int{domain.DimGlobal: 2}})
	var facts []domain.PlayFact
	for i := range 10 {
		facts = append(facts, fact(i, fmt.Sprintf("trk-%d", i%3), t0.Add(time.Duration(i)*time.Minute)))
	}
	// ingest 在套用到一半時當掉，重新啟動後從較早的 offset 再讀一次。
	s.Apply(0, 0, facts[:6])
	s.Apply(0, 3, facts[3:])

	got := s.Counts(domain.DimGlobal, Hour, t0, t0.Add(time.Hour))
	if want := map[string]int64{"trk-0": 4, "trk-1": 3, "trk-2": 3}; !equal(got, want) {
		t.Fatalf("counts = %v, want %v", got, want)
	}
	if s.MinApplied(0) != 10 {
		t.Fatalf("MinApplied = %d", s.MinApplied(0))
	}
	minute := s.Counts("country:US", Minute, t0.Add(4*time.Minute), t0.Add(5*time.Minute))
	if !equal(minute, map[string]int64{"trk-1": 1}) {
		t.Fatalf("minute 4 = %v", minute)
	}
}

func TestTopKFromSubShardsMatchesBruteForce(t *testing.T) {
	s := NewStore(Config{Shards: 16, Hot: map[string]int{domain.DimGlobal: 8, "country:US": 4}})
	rng := rand.New(rand.NewPCG(1, 2))
	countries := []string{"US", "US", "TW", "JP"}
	var facts []domain.PlayFact
	for i := range 20_000 {
		f := fact(i, fmt.Sprintf("trk-%03d", int(300*rng.Float64()*rng.Float64())), t0.Add(time.Duration(rng.IntN(3600))*time.Second))
		f.Country = countries[rng.IntN(len(countries))]
		facts = append(facts, f)
	}
	s.Apply(0, 0, facts)

	for _, dim := range []string{domain.DimGlobal, "country:US", "country:TW", "genre:pop"} {
		want := make(map[string]int64)
		for _, f := range facts {
			if dims := domain.Dimensions(f); slices.Contains(dims[:], dim) {
				want[f.TrackID]++
			}
		}
		all := make([]TrackCount, 0, len(want))
		for track, n := range want {
			all = append(all, TrackCount{TrackID: track, Plays: n})
		}
		slices.SortFunc(all, compare)
		got := s.TopK(dim, Minute, t0, t0.Add(time.Hour), 25)
		if !slices.Equal(got, all[:25]) {
			t.Fatalf("%s: top 25 = %v\nwant %v", dim, got, all[:25])
		}
	}
}

func TestReplaceHourOverwritesStreamCounts(t *testing.T) {
	s := NewStore(Config{Shards: 4, Hot: map[string]int{domain.DimGlobal: 2}})
	s.Apply(0, 0, []domain.PlayFact{fact(0, "trk-a", t0), fact(1, "trk-a", t0), fact(2, "trk-b", t0.Add(time.Minute))})

	s.ReplaceHour(domain.DimGlobal, t0, map[time.Time]map[string]int64{t0: {"trk-a": 1}}, map[string]int64{"trk-a": 1})

	if got := s.Counts(domain.DimGlobal, Hour, t0, t0.Add(time.Hour)); !equal(got, map[string]int64{"trk-a": 1}) {
		t.Fatalf("hourly = %v", got)
	}
	if got := s.Counts(domain.DimGlobal, Minute, t0, t0.Add(time.Hour)); !equal(got, map[string]int64{"trk-a": 1}) {
		t.Fatalf("minutes = %v", got)
	}
	// 其他維度不受影響。
	if got := s.Counts("country:US", Hour, t0, t0.Add(time.Hour)); !equal(got, map[string]int64{"trk-a": 2, "trk-b": 1}) {
		t.Fatalf("country:US = %v", got)
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
