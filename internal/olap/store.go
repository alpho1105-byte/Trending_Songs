package olap

import (
	"container/heap"
	"hash/fnv"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"trending-songs/internal/domain"
)

// Granularity 是預先彙總的時間粒度（materialized view）：
// 過去一小時的排行用分鐘桶加總，過去一天的排行用小時桶加總。
type Granularity time.Duration

const (
	Minute = Granularity(time.Minute)
	Hour   = Granularity(time.Hour)
)

func (g Granularity) start(t time.Time) int64 {
	return t.Truncate(time.Duration(g)).Unix()
}

type TrackCount struct {
	TrackID string `json:"trackId"`
	Plays   int64  `json:"plays"`
}

type Config struct {
	Shards int
	// Hot 是預期會很熱的維度要拆成幾個 sub-shard，例如 global 或 country:US。
	Hot map[string]int
}

type bucket struct {
	dim   string
	start int64
}

type shard struct {
	mu      sync.RWMutex
	minute  map[bucket]map[string]int64
	hour    map[bucket]map[string]int64
	applied map[int]uint64
	facts   int64
}

func (s *shard) table(g Granularity) map[bucket]map[string]int64 {
	if g == Hour {
		return s.hour
	}
	return s.minute
}

// Store 是依維度分片的 OLAP 儲存，保存每個維度、每首歌在每個時間桶的播放數。
//
// 分片鍵是維度（例如 country:TW），查詢一個維度只會碰到少數 shard。熱門維度（global、country:US）
// 會依歌曲的雜湊再拆成數個 sub-shard，避免單一 shard 吃下大部分流量。
// 同一首歌永遠落在同一個 sub-shard，所以每個 sub-shard 各自算 top K 再合併，結果仍然精確。
type Store struct {
	cfg       Config
	shards    []*shard
	watermark atomic.Int64

	dimsMu sync.RWMutex
	dims   map[string]struct{}
}

func NewStore(cfg Config) *Store {
	s := &Store{cfg: cfg, shards: make([]*shard, cfg.Shards), dims: make(map[string]struct{})}
	for i := range s.shards {
		s.shards[i] = &shard{minute: make(map[bucket]map[string]int64), hour: make(map[bucket]map[string]int64), applied: make(map[int]uint64)}
	}
	return s
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func (s *Store) subShards(dim string) int {
	return max(1, s.cfg.Hot[dim])
}

func (s *Store) route(dim, trackID string) int {
	base := int(hash(dim) % uint32(len(s.shards)))
	sub := s.subShards(dim)
	if sub == 1 {
		return base
	}
	return (base + int(hash(trackID)%uint32(sub))) % len(s.shards)
}

// shardsOf 回傳存放某個維度的所有 shard。
func (s *Store) shardsOf(dim string) []int {
	base := int(hash(dim) % uint32(len(s.shards)))
	n := min(s.subShards(dim), len(s.shards))
	out := make([]int, n)
	for i := range out {
		out[i] = (base + i) % len(s.shards)
	}
	return out
}

type item struct {
	offset uint64
	dim    string
	fact   domain.PlayFact
}

// Apply 套用 PlayFacts topic 某個分區從 offset from 開始的一批資料。
// 每個 shard 各自記錄每個分區已經套用到哪個 offset，只套用比它新的資料：
// ingest 中途當掉、重新從最小的進度讀起時，已經寫過的 shard 會自動跳過，不會重複計數。
func (s *Store) Apply(part int, from uint64, facts []domain.PlayFact) {
	if len(facts) == 0 {
		return
	}
	end := from + uint64(len(facts))
	perShard := make([][]item, len(s.shards))
	newDims := make(map[string]struct{})
	for i, f := range facts {
		for _, dim := range domain.Dimensions(f) {
			idx := s.route(dim, f.TrackID)
			perShard[idx] = append(perShard[idx], item{offset: from + uint64(i), dim: dim, fact: f})
			newDims[dim] = struct{}{}
		}
		for {
			cur := s.watermark.Load()
			if f.At.UnixMilli() <= cur || s.watermark.CompareAndSwap(cur, f.At.UnixMilli()) {
				break
			}
		}
	}
	for idx, sh := range s.shards {
		sh.mu.Lock()
		last := sh.applied[part]
		for _, it := range perShard[idx] {
			if it.offset < last {
				continue
			}
			add(sh.minute, bucket{it.dim, Minute.start(it.fact.At)}, it.fact.TrackID, 1)
			add(sh.hour, bucket{it.dim, Hour.start(it.fact.At)}, it.fact.TrackID, 1)
			sh.facts++
		}
		sh.applied[part] = max(last, end)
		sh.mu.Unlock()
	}
	s.dimsMu.Lock()
	for d := range newDims {
		s.dims[d] = struct{}{}
	}
	s.dimsMu.Unlock()
}

func add(table map[bucket]map[string]int64, b bucket, track string, n int64) {
	m := table[b]
	if m == nil {
		m = make(map[string]int64)
		table[b] = m
	}
	m[track] += n
}

// MinApplied 回傳所有 shard 中，某個分區最落後的進度。ingest 重新啟動時從這裡開始讀。
func (s *Store) MinApplied(part int) uint64 {
	var low uint64
	for i, sh := range s.shards {
		sh.mu.RLock()
		v := sh.applied[part]
		sh.mu.RUnlock()
		if i == 0 || v < low {
			low = v
		}
	}
	return low
}

// Watermark 回傳目前看過最新的播放時間（event time）。排行的時間窗以它為結尾。
func (s *Store) Watermark() time.Time {
	return time.UnixMilli(s.watermark.Load()).UTC()
}

func (s *Store) Dims() []string {
	s.dimsMu.RLock()
	defer s.dimsMu.RUnlock()
	out := make([]string, 0, len(s.dims))
	for d := range s.dims {
		out = append(out, d)
	}
	slices.Sort(out)
	return out
}

// Counts 回傳某維度在 [from, to) 期間每首歌的播放數。
func (s *Store) Counts(dim string, g Granularity, from, to time.Time) map[string]int64 {
	out := make(map[string]int64)
	for _, idx := range s.shardsOf(dim) {
		for track, n := range s.shards[idx].counts(dim, g, from, to) {
			out[track] += n
		}
	}
	return out
}

func (sh *shard) counts(dim string, g Granularity, from, to time.Time) map[string]int64 {
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	out := make(map[string]int64)
	step := time.Duration(g)
	for t := from.Truncate(step); t.Before(to); t = t.Add(step) {
		for track, n := range sh.table(g)[bucket{dim, t.Unix()}] {
			out[track] += n
		}
	}
	return out
}

// TopK 回傳某維度在 [from, to) 期間播放數最高的 k 首歌，同分時依歌曲 ID 排序。
func (s *Store) TopK(dim string, g Granularity, from, to time.Time, k int) []TrackCount {
	var merged []TrackCount
	for _, idx := range s.shardsOf(dim) {
		merged = append(merged, topK(s.shards[idx].counts(dim, g, from, to), k)...)
	}
	slices.SortFunc(merged, compare)
	return merged[:min(k, len(merged))]
}

// ReplaceHour 用批次重算的結果取代某個維度在某一小時內的分鐘桶與小時桶。
func (s *Store) ReplaceHour(dim string, hourStart time.Time, minutes map[time.Time]map[string]int64, hourly map[string]int64) {
	for _, idx := range s.shardsOf(dim) {
		sh := s.shards[idx]
		sh.mu.Lock()
		for m := hourStart; m.Before(hourStart.Add(time.Hour)); m = m.Add(time.Minute) {
			delete(sh.minute, bucket{dim, m.Unix()})
		}
		delete(sh.hour, bucket{dim, hourStart.Unix()})
		sh.mu.Unlock()
	}
	for m, counts := range minutes {
		for track, n := range counts {
			sh := s.shards[s.route(dim, track)]
			sh.mu.Lock()
			add(sh.minute, bucket{dim, m.Unix()}, track, n)
			sh.mu.Unlock()
		}
	}
	for track, n := range hourly {
		sh := s.shards[s.route(dim, track)]
		sh.mu.Lock()
		add(sh.hour, bucket{dim, hourStart.Unix()}, track, n)
		sh.mu.Unlock()
	}
}

// ShardLoad 回傳每個 shard 收到的播放數，用來觀察熱點。
func (s *Store) ShardLoad() []int64 {
	out := make([]int64, len(s.shards))
	for i, sh := range s.shards {
		sh.mu.RLock()
		out[i] = sh.facts
		sh.mu.RUnlock()
	}
	return out
}

func compare(a, b TrackCount) int {
	if a.Plays != b.Plays {
		if a.Plays > b.Plays {
			return -1
		}
		return 1
	}
	if a.TrackID < b.TrackID {
		return -1
	}
	if a.TrackID > b.TrackID {
		return 1
	}
	return 0
}

// topK 用大小為 k 的 min-heap 從計數中挑出前 k 名，時間 O(n log k)。
func topK(counts map[string]int64, k int) []TrackCount {
	h := &minHeap{}
	for track, n := range counts {
		tc := TrackCount{TrackID: track, Plays: n}
		if h.Len() < k {
			heap.Push(h, tc)
		} else if compare(tc, (*h)[0]) < 0 {
			(*h)[0] = tc
			heap.Fix(h, 0)
		}
	}
	out := []TrackCount(*h)
	slices.SortFunc(out, compare)
	return out
}

// minHeap 的頂端是目前前 k 名中「最差」的那一首。
type minHeap []TrackCount

func (h minHeap) Len() int           { return len(h) }
func (h minHeap) Less(i, j int) bool { return compare(h[i], h[j]) > 0 }
func (h minHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *minHeap) Push(x any)        { *h = append(*h, x.(TrackCount)) }
func (h *minHeap) Pop() any {
	old := *h
	x := old[len(old)-1]
	*h = old[:len(old)-1]
	return x
}
