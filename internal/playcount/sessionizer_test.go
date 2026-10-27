package playcount

import (
	"fmt"
	"testing"
	"time"

	"trending-songs/internal/domain"
)

var t0 = time.Date(2026, 10, 27, 21, 0, 0, 0, time.UTC)

type step struct {
	at    time.Duration
	pos   int64
	state domain.State
}

// play 產生 [from, to] 之間每 5 秒一筆的播放中事件，位置從 pos 開始隨時間前進。
func play(from, to time.Duration, pos int64) []step {
	var out []step
	for at := from; at <= to; at += 5 * time.Second {
		st := domain.StateHeartbeat
		if at == from {
			st = domain.StatePlay
		}
		out = append(out, step{at, pos + (at - from).Milliseconds(), st})
	}
	return out
}

func events(session string, steps ...step) []domain.Event {
	out := make([]domain.Event, len(steps))
	for i, s := range steps {
		out[i] = domain.Event{EventID: fmt.Sprintf("%s-%d", session, i), SessionID: session, UserID: "u1", TrackID: "trk-1",
			Country: "TW", PositionMs: s.pos, State: s.state, ClientTs: t0.Add(s.at)}
	}
	return out
}

func process(s *Sessionizer, evs []domain.Event) []domain.PlayFact {
	var facts []domain.PlayFact
	for _, e := range evs {
		if f, ok := s.Process(e); ok {
			facts = append(facts, f)
		}
	}
	return facts
}

func TestCountsOnceWhenListenedTimeReachesThreshold(t *testing.T) {
	s := NewSessionizer(DefaultRules())
	evs := events("s1", play(0, 90*time.Second, 0)...)
	// 每筆事件都重送一次，模擬 client 重試。
	var doubled []domain.Event
	for _, e := range evs {
		doubled = append(doubled, e, e)
	}
	facts := process(s, doubled)
	if len(facts) != 1 || facts[0].PlayID != "s1" || !facts[0].At.Equal(t0.Add(30*time.Second)) {
		t.Fatalf("facts = %+v", facts)
	}
}

func TestTimeNotActuallyListenedDoesNotCount(t *testing.T) {
	seek := append(play(0, 0, 0), step{3 * time.Second, 175_000, domain.StateSeek})
	seek = append(seek, play(8*time.Second, 8*time.Second, 180_000)[0])
	paused := append(play(0, 20*time.Second, 0), step{20 * time.Second, 20_000, domain.StatePause})
	paused = append(paused, play(50*time.Second, 55*time.Second, 20_000)...)
	gap := append(play(0, 10*time.Second, 0), play(40*time.Second, 55*time.Second, 40_000)...)

	for name, steps := range map[string][]step{"seek to the end": seek, "pause": paused, "network gap": gap} {
		if facts := process(NewSessionizer(DefaultRules()), events("s1", steps...)); len(facts) != 0 {
			t.Errorf("%s: counted %+v", name, facts)
		}
	}

	// 暫停前後實際聽滿 30 秒才算，時間點是第 30 秒聽完的那一刻。
	s := NewSessionizer(DefaultRules())
	facts := process(s, events("s1", append(paused, step{60 * time.Second, 30_000, domain.StateHeartbeat})...))
	if len(facts) != 1 || !facts[0].At.Equal(t0.Add(60*time.Second)) {
		t.Fatalf("facts = %+v", facts)
	}
}

func TestOutOfOrderEventIsIgnored(t *testing.T) {
	s := NewSessionizer(DefaultRules())
	evs := events("s1", play(0, 25*time.Second, 0)...)
	process(s, evs)
	stale := domain.Event{EventID: "s1-late", SessionID: "s1", PositionMs: 200_000, State: domain.StateHeartbeat, ClientTs: t0.Add(10 * time.Second)}
	if facts := process(s, []domain.Event{stale}); len(facts) != 0 {
		t.Fatalf("stale event counted: %+v", facts)
	}
}

// 回歸測試：session 狀態被清掉後，client 很久之後才補送的事件不能讓同一次播放再算一次。
func TestLateEventsAfterEvictionDoNotCountAgain(t *testing.T) {
	s := NewSessionizer(DefaultRules())
	evs := events("s1", play(0, 120*time.Second, 0)...)
	if facts := process(s, evs[:10]); len(facts) != 1 {
		t.Fatalf("first part: facts = %+v", facts)
	}
	s.Evict(t0.Add(time.Hour), t0.Add(-24*time.Hour))
	if len(s.sessions) != 0 || len(s.counted) != 1 {
		t.Fatalf("after evict: sessions=%d counted=%d", len(s.sessions), len(s.counted))
	}
	if facts := process(s, evs[5:]); len(facts) != 0 {
		t.Fatalf("late events counted again: %+v", facts)
	}

	s.Evict(t0.Add(time.Hour), t0.Add(time.Hour))
	if len(s.counted) != 0 {
		t.Fatalf("counted records older than the retention should be dropped, got %d", len(s.counted))
	}
}

func TestSnapshotIsIndependentOfLaterProcessing(t *testing.T) {
	s := NewSessionizer(DefaultRules())
	evs := events("s1", play(0, 60*time.Second, 0)...)
	process(s, evs[:3])
	snap := s.Snapshot()

	if facts := process(s, evs[3:]); len(facts) != 1 {
		t.Fatalf("original: facts = %+v", facts)
	}
	// 從 snapshot 重算同一段事件，結果必須相同：這是 checkpoint 還原後重放的前提。
	if facts := process(snap, evs[3:]); len(facts) != 1 || !facts[0].At.Equal(t0.Add(30*time.Second)) {
		t.Fatalf("snapshot replay: facts = %+v", facts)
	}
}

func TestCapPerListener(t *testing.T) {
	var facts []domain.PlayFact
	for i := range 7 {
		facts = append(facts, domain.PlayFact{PlayID: fmt.Sprintf("bot-%d", i), UserID: "bot", TrackID: "trk-1", At: t0.Add(time.Duration(i) * time.Minute)})
	}
	facts = append(facts,
		domain.PlayFact{PlayID: "bot-next-hour", UserID: "bot", TrackID: "trk-1", At: t0.Add(time.Hour)},
		domain.PlayFact{PlayID: "bot-other-track", UserID: "bot", TrackID: "trk-2", At: t0},
		domain.PlayFact{PlayID: "fan", UserID: "fan", TrackID: "trk-1", At: t0},
	)
	kept, removed := CapPerListener(facts, 5)
	if removed != 2 || len(kept) != 8 {
		t.Fatalf("kept %d, removed %d", len(kept), removed)
	}
	for _, f := range kept {
		if f.PlayID == "bot-5" || f.PlayID == "bot-6" {
			t.Fatalf("the earliest plays should be kept, but %s survived", f.PlayID)
		}
	}
}
