package olap

import (
	"context"
	"sync"
	"time"

	"trending-songs/internal/domain"
	"trending-songs/internal/metrics"
	"trending-songs/internal/topic"
)

// Ingest 把 PlayFacts topic 的資料以微批次寫進 OLAP。它本身不保存進度：
// 每次啟動都向 Store 詢問每個分區最落後的 shard 套用到哪裡，從那裡繼續讀。
type Ingest struct {
	facts   *topic.Topic[domain.PlayFact]
	store   *Store
	batch   int
	latency *metrics.Latency
}

func NewIngest(facts *topic.Topic[domain.PlayFact], store *Store, batch int, latency *metrics.Latency) *Ingest {
	return &Ingest{facts: facts, store: store, batch: batch, latency: latency}
}

func (in *Ingest) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for part := range in.facts.Partitions() {
		wg.Add(1)
		go func() {
			defer wg.Done()
			offset := in.store.MinApplied(part)
			for {
				facts, err := in.facts.Read(ctx, part, offset, in.batch)
				if err != nil {
					return
				}
				in.store.Apply(part, offset, facts)
				now := time.Now()
				for _, f := range facts {
					// 從伺服器收到觸發這次播放的事件，到它出現在 OLAP 裡的時間。
					in.latency.Observe(now.Sub(f.ReceivedAt))
				}
				offset += uint64(len(facts))
			}
		}()
	}
	wg.Wait()
}
