package ingest

import (
	"context"
	"testing"
	"time"

	"trending-songs/internal/catalog"
	"trending-songs/internal/domain"
	"trending-songs/internal/topic"
)

func TestAcceptValidatesEventsAndUsesTheAuthenticatedIdentity(t *testing.T) {
	now := time.Date(2026, 10, 27, 21, 0, 0, 0, time.UTC)
	events := topic.New[domain.Event]("events", 4)
	s := NewService(events, catalog.Generate(10, 1))
	s.now = func() time.Time { return now }

	ok := EventInput{EventID: "e1", SessionID: "s1", TrackID: "trk-00001", PositionMs: 5_000, State: "heartbeat", ClientTs: now.Add(-time.Minute).UnixMilli()}
	offline := ok
	offline.EventID, offline.ClientTs = "e2", now.Add(-48*time.Hour).UnixMilli()
	bad := map[string]func(*EventInput){
		"missing event id": func(e *EventInput) { e.EventID = "" },
		"unknown track":    func(e *EventInput) { e.TrackID = "trk-99999" },
		"unknown state":    func(e *EventInput) { e.State = "rewind" },
		"negative pos":     func(e *EventInput) { e.PositionMs = -1 },
		"from the future":  func(e *EventInput) { e.ClientTs = now.Add(10 * time.Minute).UnixMilli() },
		"too old":          func(e *EventInput) { e.ClientTs = now.Add(-8 * 24 * time.Hour).UnixMilli() },
	}
	in := []EventInput{ok, offline}
	for _, mutate := range bad {
		e := ok
		mutate(&e)
		in = append(in, e)
	}

	accepted, rejected := s.Accept("user-1", "TW", in)
	if accepted != 2 || rejected != len(bad) {
		t.Fatalf("accepted=%d rejected=%d", accepted, rejected)
	}
	part := events.PartitionFor("s1")
	got, _ := events.Read(context.Background(), part, 0, 10)
	if len(got) != 2 {
		t.Fatalf("events of one session should share a partition, got %d in partition %d", len(got), part)
	}
	if e := got[0]; e.UserID != "user-1" || e.Country != "TW" || !e.ReceivedAt.Equal(now) || e.State != domain.StateHeartbeat {
		t.Fatalf("event = %+v", e)
	}
}
