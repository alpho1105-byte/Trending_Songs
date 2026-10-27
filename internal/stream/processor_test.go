package stream

import (
	"context"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"trending-songs/internal/catalog"
	"trending-songs/internal/domain"
	"trending-songs/internal/topic"
)

var t0 = time.Date(2026, 10, 27, 21, 0, 0, 0, time.UTC)

// session 產生一次從頭聽 d 的播放：開頭一筆 play、之後每 5 秒一筆 heartbeat。
func session(id string, start time.Time, d time.Duration) []domain.Event {
	var out []domain.Event
	for at := time.Duration(0); at <= d; at += 5 * time.Second {
		st := domain.StateHeartbeat
		if at == 0 {
			st = domain.StatePlay
		}
		out = append(out, domain.Event{EventID: fmt.Sprintf("%s-%d", id, len(out)), SessionID: id, UserID: "u1",
			TrackID: "trk-00001", Country: "TW", PositionMs: at.Milliseconds(), State: st, ClientTs: start.Add(at)})
	}
	return out
}

type harness struct {
	t      *testing.T
	events *topic.Topic[domain.Event]
	out    *topic.Topic[domain.PlayFact]
	cps    *Checkpoints
}

func newHarness(t *testing.T) *harness {
	return &harness{t: t, events: topic.New[domain.Event]("events", 1), out: topic.New[domain.PlayFact]("facts", 1), cps: NewCheckpoints()}
}

// start 啟動一個 processor instance，回傳的函式模擬它當掉：直接結束，不做最後一次 commit。
func (h *harness) start(cfg Config) (*Processor, func()) {
	p := NewProcessor(h.events, h.out, h.cps, catalog.Generate(10, 1), []int{0}, cfg, slog.New(slog.DiscardHandler))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		p.Run(ctx)
		close(done)
	}()
	return p, func() {
		cancel()
		<-done
	}
}

func (h *harness) waitCommitted() {
	h.t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		if cp, ok := h.cps.Load(0); ok && cp.Offset == h.events.End(0) {
			return
		}
		if time.Now().After(deadline) {
			h.t.Fatal("processor did not commit everything")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *harness) facts() map[string]int {
	out := make(map[string]int)
	if h.out.End(0) == 0 {
		return out
	}
	records, _ := h.out.Read(context.Background(), 0, 0, int(h.out.End(0)))
	for _, f := range records {
		out[f.PlayID]++
	}
	return out
}

func fastConfig() Config {
	cfg := DefaultConfig()
	cfg.CheckpointInterval = 10 * time.Millisecond
	return cfg
}

func TestCrashBeforeCommitReplaysFromCheckpointExactlyOnce(t *testing.T) {
	h := newHarness(t)
	for i := range 3 {
		h.events.Append(0, session(fmt.Sprintf("s%d", i), t0.Add(time.Duration(i)*time.Minute), time.Minute)...)
	}

	// 第一個 instance 處理完所有事件，但在 commit 前就當掉：輸出與 checkpoint 都不能留下任何東西。
	never := DefaultConfig()
	never.CheckpointEvery, never.CheckpointInterval = 1<<30, time.Hour
	p, crash := h.start(never)
	deadline := time.Now().Add(5 * time.Second)
	for processed, _, _ := p.Stats(); processed < int64(h.events.End(0)); processed, _, _ = p.Stats() {
		if time.Now().After(deadline) {
			t.Fatal("first instance did not read the events")
		}
		time.Sleep(5 * time.Millisecond)
	}
	crash()
	if _, ok := h.cps.Load(0); ok || h.out.End(0) != 0 {
		t.Fatalf("uncommitted work leaked: facts=%d", h.out.End(0))
	}

	_, crash = h.start(fastConfig())
	h.waitCommitted()
	crash()

	// 再啟動一個 instance，從 checkpoint 接續；重送的舊事件不能讓已計過的 session 再算一次。
	h.events.Append(0, session("s0", t0, time.Minute)[4:]...)
	h.events.Append(0, session("s3", t0.Add(5*time.Minute), time.Minute)...)
	_, crash = h.start(fastConfig())
	defer crash()
	h.waitCommitted()

	facts := h.facts()
	if len(facts) != 4 {
		t.Fatalf("facts = %v", facts)
	}
	for id, n := range facts {
		if n != 1 {
			t.Fatalf("%s emitted %d times", id, n)
		}
	}
}

func TestLateRetryAfterStateEvictionIsNotCountedAgain(t *testing.T) {
	h := newHarness(t)
	_, crash := h.start(fastConfig())
	defer crash()

	s0 := session("s0", t0, 2*time.Minute)
	h.events.Append(0, s0[:10]...)
	// 兩小時後的播放把 watermark 往前推，s0 的完整狀態在下一次 commit 時被清掉。
	h.events.Append(0, session("s1", t0.Add(2*time.Hour), time.Minute)...)
	h.waitCommitted()
	h.events.Append(0, s0[5:]...)
	h.waitCommitted()

	if facts := h.facts(); len(facts) != 2 || facts["s0"] != 1 {
		t.Fatalf("facts = %v", facts)
	}
}

func TestEventsOlderThanRetentionAreLeftToTheBatchLayer(t *testing.T) {
	h := newHarness(t)
	cfg := fastConfig()
	cfg.CountedRetention = time.Hour
	p, crash := h.start(cfg)
	defer crash()

	h.events.Append(0, session("now", t0.Add(3*time.Hour), time.Minute)...)
	old := session("old", t0, time.Minute)
	h.events.Append(0, old...)
	h.waitCommitted()

	if facts := h.facts(); len(facts) != 1 || facts["now"] != 1 {
		t.Fatalf("facts = %v", facts)
	}
	if _, _, tooLate := p.Stats(); tooLate != int64(len(old)) {
		t.Fatalf("tooLate = %d, want %d", tooLate, len(old))
	}
}
