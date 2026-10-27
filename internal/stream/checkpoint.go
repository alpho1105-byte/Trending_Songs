package stream

import (
	"sync"
	"sync/atomic"
	"time"

	"trending-songs/internal/domain"
	"trending-songs/internal/playcount"
	"trending-songs/internal/topic"
)

// Checkpoint 是一個分區的處理進度：讀到哪個 offset、當時的 session 狀態與 watermark。
type Checkpoint struct {
	Offset    uint64
	State     *playcount.Sessionizer
	Watermark time.Time
}

// Checkpoints 是 checkpoint 的持久化儲存（正式環境是 S3 / HDFS 上的 state backend）。
//
// Commit 在同一個交易裡做兩件事：把這段期間產生的 PlayFacts 寫進輸出 topic，以及保存 checkpoint。
// 兩者要嘛一起成功、要嘛都沒發生，對應 Kafka 的 transactional producer 或 Flink 的 two-phase commit sink。
// processor 在兩次 commit 之間當掉，就從上一個 checkpoint 重新讀取並重算，輸出的 PlayFacts 不會重複也不會遺失。
type Checkpoints struct {
	mu      sync.Mutex
	data    map[int]Checkpoint
	commits atomic.Int64
}

func NewCheckpoints() *Checkpoints {
	return &Checkpoints{data: make(map[int]Checkpoint)}
}

func (c *Checkpoints) Commit(part int, cp Checkpoint, out *topic.Topic[domain.PlayFact], facts []domain.PlayFact) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(facts) > 0 {
		out.Append(part%out.Partitions(), facts...)
	}
	c.data[part] = cp
	c.commits.Add(1)
}

func (c *Checkpoints) Load(part int) (Checkpoint, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	cp, ok := c.data[part]
	return cp, ok
}

func (c *Checkpoints) Commits() int64 {
	return c.commits.Load()
}
