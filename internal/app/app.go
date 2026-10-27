package app

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"trending-songs/internal/batch"
	"trending-songs/internal/catalog"
	"trending-songs/internal/domain"
	"trending-songs/internal/ingest"
	"trending-songs/internal/metrics"
	"trending-songs/internal/olap"
	"trending-songs/internal/stream"
	"trending-songs/internal/topic"
	"trending-songs/internal/topk"
)

type Options struct {
	Catalog         *catalog.Catalog
	EventPartitions int
	// Processors 是 stream processor 的 instance 數，事件分區平均分配給它們。
	Processors     int
	Stream         stream.Config
	OLAP           olap.Config
	IngestBatch    int
	TopK           int
	RefreshEvery   time.Duration
	CapPerListener int
	Logger         *slog.Logger
}

func DefaultOptions(c *catalog.Catalog) Options {
	return Options{
		Catalog: c, EventPartitions: 8, Processors: 4, Stream: stream.DefaultConfig(),
		OLAP:        olap.Config{Shards: 16, Hot: map[string]int{domain.DimGlobal: 8, "country:US": 4}},
		IngestBatch: 2_000, TopK: 100, RefreshEvery: time.Second, CapPerListener: 5, Logger: slog.Default(),
	}
}

// App 是 composition root。資料流：
//
//	裝置 → Ingest（無狀態）→ events topic ─┬→ stream processors → PlayFacts topic → OLAP ingest → OLAP → Top-K 快取 → API
//	                                        └→ 資料湖 → 批次重算 → 校正 OLAP
type App struct {
	opts        Options
	Catalog     *catalog.Catalog
	Events      *topic.Topic[domain.Event]
	Facts       *topic.Topic[domain.PlayFact]
	Ingest      *ingest.Service
	Checkpoints *stream.Checkpoints
	OLAP        *olap.Store
	Cache       *topk.Cache
	Refresher   *topk.Refresher
	Lake        *batch.Lake
	LakeWriter  *batch.Writer
	Batch       *batch.Job
	// Freshness 是事件抵達伺服器到對應的播放出現在 OLAP 的延遲。
	Freshness *metrics.Latency

	mu         sync.Mutex
	ctx        context.Context
	wg         sync.WaitGroup
	processors []*instance[*stream.Processor]
	olapIngest *instance[*olap.Ingest]
}

type instance[T interface{ Run(context.Context) }] struct {
	run    T
	cancel context.CancelFunc
	done   chan struct{}
}

func New(opts Options) *App {
	a := &App{
		opts:        opts,
		Catalog:     opts.Catalog,
		Events:      topic.New[domain.Event]("listen-events", opts.EventPartitions),
		Facts:       topic.New[domain.PlayFact]("play-facts", opts.EventPartitions),
		Checkpoints: stream.NewCheckpoints(),
		OLAP:        olap.NewStore(opts.OLAP),
		Cache:       topk.NewCache(),
		Lake:        batch.NewLake(),
		Freshness:   &metrics.Latency{},
	}
	a.Ingest = ingest.NewService(a.Events, opts.Catalog)
	a.Refresher = topk.NewRefresher(a.OLAP, a.Cache, opts.TopK, opts.RefreshEvery)
	a.LakeWriter = batch.NewWriter(a.Events, a.Lake)
	a.Batch = batch.NewJob(a.Lake, opts.Catalog, opts.Stream.Rules, opts.CapPerListener)
	return a
}

// Run 啟動所有背景元件，阻塞直到 ctx 結束。
func (a *App) Run(ctx context.Context) {
	a.mu.Lock()
	a.ctx = ctx
	for i := range a.opts.Processors {
		a.processors = append(a.processors, a.startProcessor(i))
	}
	a.olapIngest = a.startIngest()
	a.mu.Unlock()

	for _, fn := range []func(context.Context){a.Refresher.Run, a.LakeWriter.Run} {
		a.wg.Add(1)
		go func() {
			defer a.wg.Done()
			fn(ctx)
		}()
	}
	<-ctx.Done()
	a.wg.Wait()
}

func start[T interface{ Run(context.Context) }](parent context.Context, wg *sync.WaitGroup, run T) *instance[T] {
	ctx, cancel := context.WithCancel(parent)
	inst := &instance[T]{run: run, cancel: cancel, done: make(chan struct{})}
	wg.Add(1)
	go func() {
		defer wg.Done()
		defer close(inst.done)
		run.Run(ctx)
	}()
	return inst
}

func (a *App) startProcessor(i int) *instance[*stream.Processor] {
	var parts []int
	for p := i; p < a.opts.EventPartitions; p += a.opts.Processors {
		parts = append(parts, p)
	}
	proc := stream.NewProcessor(a.Events, a.Facts, a.Checkpoints, a.Catalog, parts, a.opts.Stream,
		a.opts.Logger.With("component", "stream", "instance", i))
	return start(a.ctx, &a.wg, proc)
}

func (a *App) startIngest() *instance[*olap.Ingest] {
	return start(a.ctx, &a.wg, olap.NewIngest(a.Facts, a.OLAP, a.opts.IngestBatch, a.Freshness))
}

// CrashProcessor 讓第 i 個 stream processor 當掉（不做最後一次 checkpoint），並以新的 instance 取代。
// 新 instance 從 checkpoint 還原狀態，重新讀取當掉前尚未 commit 的事件。
func (a *App) CrashProcessor(i int) {
	a.mu.Lock()
	defer a.mu.Unlock()
	old := a.processors[i]
	old.cancel()
	<-old.done
	a.processors[i] = a.startProcessor(i)
}

// CrashOLAPIngest 讓 OLAP ingest 當掉並重新啟動，重新啟動後從各 shard 最落後的進度讀起。
func (a *App) CrashOLAPIngest() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.olapIngest.cancel()
	<-a.olapIngest.done
	a.olapIngest = a.startIngest()
}

// StreamStats 回傳目前所有 processor instance 讀過的事件數、產生的 PlayFact 數與太晚到而略過的事件數。
func (a *App) StreamStats() (processed, emitted, tooLate int64) {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, p := range a.processors {
		pr, em, tl := p.run.Stats()
		processed += pr
		emitted += em
		tooLate += tl
	}
	return processed, emitted, tooLate
}

// Drained 回傳所有元件是否已經處理完目前收到的事件。
func (a *App) Drained() bool {
	for part := range a.Events.Partitions() {
		cp, ok := a.Checkpoints.Load(part)
		end := a.Events.End(part)
		if end > 0 && (!ok || cp.Offset < end) {
			return false
		}
		if a.OLAP.MinApplied(part) < a.Facts.End(part) {
			return false
		}
	}
	return a.LakeWriter.CaughtUp()
}
