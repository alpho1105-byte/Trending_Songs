package client

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"testing"

	"trending-songs/internal/ingest"
)

type fakeSender struct {
	fail    int
	batches [][]string
}

func (s *fakeSender) Send(_ context.Context, batch []ingest.EventInput) error {
	if s.fail > 0 {
		s.fail--
		return errors.New("timeout")
	}
	var ids []string
	for _, e := range batch {
		ids = append(ids, e.EventID)
	}
	s.batches = append(s.batches, ids)
	return nil
}

func TestBufferBatchesAndKeepsOrderAcrossOfflineAndFailures(t *testing.T) {
	ctx := context.Background()
	s := &fakeSender{}
	b := NewBuffer(s, 3)
	add := func(from, to int) {
		for i := from; i < to; i++ {
			b.Add(ctx, ingest.EventInput{EventID: fmt.Sprint(i)})
		}
	}

	add(0, 2)
	if len(s.batches) != 0 {
		t.Fatalf("sent before the batch was full: %v", s.batches)
	}
	add(2, 3)

	b.SetOnline(ctx, false)
	add(3, 7)
	if len(s.batches) != 1 || b.Pending() != 4 {
		t.Fatalf("offline: batches=%v pending=%d", s.batches, b.Pending())
	}

	// 恢復連線時第一次補送失敗，事件留在本機，下次依原順序重送。
	s.fail = 1
	b.SetOnline(ctx, true)
	if b.Pending() != 4 {
		t.Fatalf("failed batch should stay pending, pending=%d", b.Pending())
	}
	if err := b.Flush(ctx); err != nil || b.Pending() != 0 {
		t.Fatalf("flush: err=%v pending=%d", err, b.Pending())
	}
	want := [][]string{{"0", "1", "2"}, {"3", "4", "5"}, {"6"}}
	if !slices.EqualFunc(s.batches, want, slices.Equal) {
		t.Fatalf("batches = %v, want %v", s.batches, want)
	}
}
