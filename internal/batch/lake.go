package batch

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"trending-songs/internal/domain"
	"trending-songs/internal/topic"
)

// Lake 是原始事件的資料湖：append-only，依事件時間的小時分區，連重複的事件都原封不動保留。
// 正式環境是 S3 上以 Parquet 存放、用 Iceberg 管理的 table。
type Lake struct {
	mu     sync.RWMutex
	byHour map[int64][]domain.Event
}

func NewLake() *Lake {
	return &Lake{byHour: make(map[int64][]domain.Event)}
}

func (l *Lake) Append(events []domain.Event) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, e := range events {
		h := e.ClientTs.Truncate(time.Hour).Unix()
		l.byHour[h] = append(l.byHour[h], e)
	}
}

// Between 回傳事件時間落在 [from, to) 的所有事件。
func (l *Lake) Between(from, to time.Time) []domain.Event {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var out []domain.Event
	for h := from.Truncate(time.Hour); h.Before(to); h = h.Add(time.Hour) {
		for _, e := range l.byHour[h.Unix()] {
			if !e.ClientTs.Before(from) && e.ClientTs.Before(to) {
				out = append(out, e)
			}
		}
	}
	return out
}

// Writer 是另一個獨立的 consumer：把事件 topic 原封不動寫進資料湖，和 stream processor 互不影響。
type Writer struct {
	events   *topic.Topic[domain.Event]
	lake     *Lake
	progress []atomic.Uint64
}

func NewWriter(events *topic.Topic[domain.Event], lake *Lake) *Writer {
	return &Writer{events: events, lake: lake, progress: make([]atomic.Uint64, events.Partitions())}
}

func (w *Writer) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for part := range w.events.Partitions() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				off := w.progress[part].Load()
				batch, err := w.events.Read(ctx, part, off, 5_000)
				if err != nil {
					return
				}
				w.lake.Append(batch)
				w.progress[part].Add(uint64(len(batch)))
			}
		}()
	}
	wg.Wait()
}

// CaughtUp 回傳資料湖是否已經收進 topic 目前所有的事件。
func (w *Writer) CaughtUp() bool {
	for part := range w.events.Partitions() {
		if w.progress[part].Load() < w.events.End(part) {
			return false
		}
	}
	return true
}
