package playcount

import (
	"slices"
	"time"

	"trending-songs/internal/domain"
)

// CapPerListener 限制同一位使用者在同一小時內對同一首歌的有效播放數，超過的部分視為人為刷流。
// 這類規則需要看完整一個時段的資料才能判斷，所以放在批次層而不是串流層。
func CapPerListener(facts []domain.PlayFact, limit int) (kept []domain.PlayFact, removed int) {
	sorted := slices.Clone(facts)
	slices.SortFunc(sorted, func(a, b domain.PlayFact) int {
		if c := a.At.Compare(b.At); c != 0 {
			return c
		}
		if a.PlayID < b.PlayID {
			return -1
		}
		return 1
	})
	type key struct {
		user, track string
		hour        time.Time
	}
	seen := make(map[key]int)
	for _, f := range sorted {
		k := key{f.UserID, f.TrackID, f.At.Truncate(time.Hour)}
		seen[k]++
		if seen[k] > limit {
			removed++
			continue
		}
		kept = append(kept, f)
	}
	return kept, removed
}
