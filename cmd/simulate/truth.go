package main

import (
	"slices"
	"time"

	"trending-songs/internal/domain"
	"trending-songs/internal/olap"
	"trending-songs/internal/playcount"
)

// truth 是正確答案：用同一套有效播放規則，在乾淨、無重複、依序的事件上算出的結果。
// 管線經歷重送、離線補送、元件當機之後，算出的數字必須和它一模一樣。
type truth struct {
	facts   []domain.PlayFact
	minutes map[string]map[time.Time]map[string]int64 // dim → 分鐘 → 歌曲 → 播放數
}

func computeTruth(w *workload, rules playcount.Rules) truth {
	s := playcount.NewSessionizer(rules)
	var facts []domain.PlayFact
	ids := make([]string, 0, len(w.clean))
	for id := range w.clean {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	for _, id := range ids {
		for _, e := range w.clean[id] {
			if f, ok := s.Process(e); ok {
				t, _ := w.catalog.Get(f.TrackID)
				f.Genre = t.Genre
				facts = append(facts, f)
			}
		}
	}
	return newTruth(facts)
}

func newTruth(facts []domain.PlayFact) truth {
	t := truth{facts: facts, minutes: make(map[string]map[time.Time]map[string]int64)}
	for _, f := range facts {
		m := f.At.Truncate(time.Minute)
		for _, dim := range domain.Dimensions(f) {
			if t.minutes[dim] == nil {
				t.minutes[dim] = make(map[time.Time]map[string]int64)
			}
			if t.minutes[dim][m] == nil {
				t.minutes[dim][m] = make(map[string]int64)
			}
			t.minutes[dim][m][f.TrackID]++
		}
	}
	return t
}

func (t truth) counts(dim string, from, to time.Time) map[string]int64 {
	out := make(map[string]int64)
	for m, tracks := range t.minutes[dim] {
		if !m.Before(from) && m.Before(to) {
			for track, n := range tracks {
				out[track] += n
			}
		}
	}
	return out
}

// topK 是暴力解：把所有歌曲排序後取前 k 名，用來比對分片合併的結果。
func topK(counts map[string]int64, k int) []olap.TrackCount {
	out := make([]olap.TrackCount, 0, len(counts))
	for track, n := range counts {
		out = append(out, olap.TrackCount{TrackID: track, Plays: n})
	}
	slices.SortFunc(out, func(a, b olap.TrackCount) int {
		if a.Plays != b.Plays {
			if a.Plays > b.Plays {
				return -1
			}
			return 1
		}
		if a.TrackID < b.TrackID {
			return -1
		}
		return 1
	})
	return out[:min(k, len(out))]
}

func equalCounts(a, b map[string]int64) bool {
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

func rankOf(tracks []olap.TrackCount, id string) int {
	for i, t := range tracks {
		if t.TrackID == id {
			return i + 1
		}
	}
	return 0
}
