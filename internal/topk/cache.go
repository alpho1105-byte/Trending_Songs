package topk

import (
	"context"
	"sync"
	"time"

	"trending-songs/internal/olap"
)

// Window 是排行的時間窗，以及用來加總的彙總粒度。
type Window struct {
	Name        string
	Span        time.Duration
	Granularity olap.Granularity
}

var Windows = []Window{
	{Name: "1h", Span: time.Hour, Granularity: olap.Minute},
	{Name: "1d", Span: 24 * time.Hour, Granularity: olap.Hour},
}

func WindowByName(name string) (Window, bool) {
	for _, w := range Windows {
		if w.Name == name {
			return w, true
		}
	}
	return Window{}, false
}

type Entry struct {
	Dim        string
	Window     string
	From, To   time.Time
	Tracks     []olap.TrackCount
	ComputedAt time.Time
}

// Cache 保存每個維度、每個時間窗目前的排行。查詢模式固定，所以由背景工作定期重算，
// API 直接讀快取，大量的查詢不會打到 OLAP。
type Cache struct {
	mu      sync.RWMutex
	entries map[string]Entry
}

func NewCache() *Cache {
	return &Cache{entries: make(map[string]Entry)}
}

func (c *Cache) Get(dim, window string) (Entry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	e, ok := c.entries[dim+"|"+window]
	return e, ok
}

func (c *Cache) set(e Entry) {
	c.mu.Lock()
	c.entries[e.Dim+"|"+e.Window] = e
	c.mu.Unlock()
}

// Refresher 定期為每個維度重算 top K。時間窗的結尾是 OLAP 目前的 watermark（event time），
// 不是牆上時鐘：離線補送的播放會算進它實際發生的時段。
type Refresher struct {
	store *olap.Store
	cache *Cache
	k     int
	every time.Duration
}

func NewRefresher(store *olap.Store, cache *Cache, k int, every time.Duration) *Refresher {
	return &Refresher{store: store, cache: cache, k: k, every: every}
}

func (r *Refresher) Run(ctx context.Context) {
	ticker := time.NewTicker(r.every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.Refresh()
		}
	}
}

func (r *Refresher) Refresh() {
	wm := r.store.Watermark()
	now := time.Now()
	for _, dim := range r.store.Dims() {
		for _, w := range Windows {
			to := wm.Truncate(time.Duration(w.Granularity)).Add(time.Duration(w.Granularity))
			from := to.Add(-w.Span)
			r.cache.set(Entry{Dim: dim, Window: w.Name, From: from, To: to,
				Tracks: r.store.TopK(dim, w.Granularity, from, to, r.k), ComputedAt: now})
		}
	}
}
