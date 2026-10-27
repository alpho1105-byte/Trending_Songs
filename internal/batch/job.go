package batch

import (
	"slices"
	"time"

	"trending-songs/internal/catalog"
	"trending-songs/internal/domain"
	"trending-songs/internal/olap"
	"trending-songs/internal/playcount"
)

// Job 是批次層：從資料湖重新計算某一小時的有效播放，並套用需要完整時段資料才能判斷的刷量規則。
// 串流層追求即時，批次層追求正確；批次結果出來後會回頭校正串流算出的排行。
type Job struct {
	lake    *Lake
	catalog *catalog.Catalog
	rules   playcount.Rules
	cap     int
	// lookback 讓跨越整點的 session（整點前開始、整點後才聽滿 30 秒）也能被完整重建。
	lookback time.Duration
}

func NewJob(lake *Lake, c *catalog.Catalog, rules playcount.Rules, capPerListener int) *Job {
	return &Job{lake: lake, catalog: c, rules: rules, cap: capPerListener, lookback: 30 * time.Minute}
}

type Result struct {
	Hour    time.Time
	Plays   int // 套用刷量規則前
	Removed int
	Minutes map[string]map[time.Time]map[string]int64
	Hourly  map[string]map[string]int64
}

func (j *Job) Recompute(hour time.Time) Result {
	events := j.lake.Between(hour.Add(-j.lookback), hour.Add(time.Hour))
	// 同一個 session 的事件依事件時間重新排序；串流端看到的也是同樣的順序。
	slices.SortStableFunc(events, func(a, b domain.Event) int {
		if a.SessionID != b.SessionID {
			if a.SessionID < b.SessionID {
				return -1
			}
			return 1
		}
		return a.ClientTs.Compare(b.ClientTs)
	})

	s := playcount.NewSessionizer(j.rules)
	var facts []domain.PlayFact
	for _, e := range events {
		f, ok := s.Process(e)
		if !ok || f.At.Before(hour) || !f.At.Before(hour.Add(time.Hour)) {
			continue
		}
		if t, found := j.catalog.Get(f.TrackID); found {
			f.Genre = t.Genre
		}
		facts = append(facts, f)
	}
	kept, removed := playcount.CapPerListener(facts, j.cap)

	r := Result{Hour: hour, Plays: len(facts), Removed: removed,
		Minutes: make(map[string]map[time.Time]map[string]int64), Hourly: make(map[string]map[string]int64)}
	for _, f := range kept {
		minute := f.At.Truncate(time.Minute)
		for _, dim := range domain.Dimensions(f) {
			if r.Minutes[dim] == nil {
				r.Minutes[dim] = make(map[time.Time]map[string]int64)
				r.Hourly[dim] = make(map[string]int64)
			}
			if r.Minutes[dim][minute] == nil {
				r.Minutes[dim][minute] = make(map[string]int64)
			}
			r.Minutes[dim][minute][f.TrackID]++
			r.Hourly[dim][f.TrackID]++
		}
	}
	return r
}

// Reconcile 用批次結果取代 OLAP 中那一小時的資料，回傳校正前串流算出的總播放數。
func Reconcile(store *olap.Store, r Result) (streamPlays int64) {
	for _, n := range store.Counts(domain.DimGlobal, olap.Hour, r.Hour, r.Hour.Add(time.Hour)) {
		streamPlays += n
	}
	dims := make(map[string]struct{})
	for _, d := range store.Dims() {
		dims[d] = struct{}{}
	}
	for d := range r.Hourly {
		dims[d] = struct{}{}
	}
	for dim := range dims {
		store.ReplaceHour(dim, r.Hour, r.Minutes[dim], r.Hourly[dim])
	}
	return streamPlays
}
