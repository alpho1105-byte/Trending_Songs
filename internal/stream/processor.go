package stream

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"trending-songs/internal/catalog"
	"trending-songs/internal/domain"
	"trending-songs/internal/playcount"
	"trending-songs/internal/topic"
)

type Config struct {
	Rules playcount.Rules
	// CheckpointEvery 與 CheckpointInterval 決定多久 commit 一次：越頻繁，當機後要重算的越少，
	// 下游也越早看到結果；代價是每次 commit 都要複製一份狀態。
	CheckpointEvery    int
	CheckpointInterval time.Duration
	// AllowedLateness 是 session 在最後一筆事件之後還要保留多久（以 event time 計），
	// 讓離線補送的事件還接得上原本的 session。
	AllowedLateness time.Duration
	// CountedRetention 是已計過的 session 紀錄要保留多久。比這更晚到的事件串流層直接略過，
	// 否則無法判斷它屬於的 session 是否已經計過；這些事件仍在 raw lake 裡，由批次層補算。
	CountedRetention time.Duration
	ReadBatch        int
}

func DefaultConfig() Config {
	return Config{
		Rules: playcount.DefaultRules(), CheckpointEvery: 2_000, CheckpointInterval: 200 * time.Millisecond,
		AllowedLateness: 30 * time.Minute, CountedRetention: 24 * time.Hour, ReadBatch: 500,
	}
}

// Processor 是 stream processor 的一個 instance，負責一組事件分區：
// 去重、依 session 累積收聽時間、產生 PlayFacts，並定期 checkpoint。
// 同一個 session 的事件都在同一個分區，所以狀態只需要放在本機記憶體。
type Processor struct {
	events  *topic.Topic[domain.Event]
	out     *topic.Topic[domain.PlayFact]
	cps     *Checkpoints
	catalog *catalog.Catalog
	parts   []int
	cfg     Config
	log     *slog.Logger

	processed atomic.Int64
	emitted   atomic.Int64
	tooLate   atomic.Int64
}

func NewProcessor(events *topic.Topic[domain.Event], out *topic.Topic[domain.PlayFact], cps *Checkpoints,
	c *catalog.Catalog, parts []int, cfg Config, log *slog.Logger) *Processor {
	return &Processor{events: events, out: out, cps: cps, catalog: c, parts: parts, cfg: cfg, log: log}
}

// Run 處理所有負責的分區，直到 ctx 結束。ctx 結束時不會做最後一次 commit：
// 這正是當機的語意，尚未 commit 的進度由下一個 instance 從 checkpoint 重算。
func (p *Processor) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, part := range p.parts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			p.run(ctx, part)
		}()
	}
	wg.Wait()
}

func (p *Processor) run(ctx context.Context, part int) {
	state := playcount.NewSessionizer(p.cfg.Rules)
	var offset uint64
	var watermark time.Time
	if cp, ok := p.cps.Load(part); ok {
		state, offset, watermark = cp.State.Snapshot(), cp.Offset, cp.Watermark
	}

	var pending []domain.PlayFact
	since, lastCommit := 0, time.Now()
	commit := func() {
		state.Evict(watermark.Add(-p.cfg.AllowedLateness), watermark.Add(-p.cfg.CountedRetention))
		p.cps.Commit(part, Checkpoint{Offset: offset, State: state.Snapshot(), Watermark: watermark}, p.out, pending)
		p.emitted.Add(int64(len(pending)))
		pending, since, lastCommit = nil, 0, time.Now()
	}

	for {
		readCtx, cancel := context.WithTimeout(ctx, p.cfg.CheckpointInterval)
		batch, err := p.events.Read(readCtx, part, offset, p.cfg.ReadBatch)
		cancel()
		if ctx.Err() != nil {
			return
		}
		if errors.Is(err, context.DeadlineExceeded) {
			if since > 0 {
				commit()
			}
			continue
		}
		for _, e := range batch {
			offset++
			since++
			if e.ClientTs.After(watermark) {
				watermark = e.ClientTs
			}
			if e.ClientTs.Before(watermark.Add(-p.cfg.CountedRetention)) {
				p.tooLate.Add(1)
				continue
			}
			if f, ok := state.Process(e); ok {
				if t, found := p.catalog.Get(f.TrackID); found {
					f.Genre = t.Genre
				}
				pending = append(pending, f)
			}
		}
		p.processed.Add(int64(len(batch)))
		if since >= p.cfg.CheckpointEvery || time.Since(lastCommit) >= p.cfg.CheckpointInterval {
			commit()
		}
	}
}

// Stats 回傳這個 instance 讀過的事件數、commit 出去的 PlayFact 數，以及太晚到而略過的事件數。
func (p *Processor) Stats() (processed, emitted, tooLate int64) {
	return p.processed.Load(), p.emitted.Load(), p.tooLate.Load()
}
