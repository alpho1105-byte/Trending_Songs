// simulate 讓 3,000 位使用者（其中 40 個是刷量機器人）聽兩小時的歌，事件經過不穩定的網路、
// 部分使用者離線後補送，途中再讓 stream processor 與 OLAP ingest 各當掉一次。
// 最後驗證：串流算出的播放數與正確答案完全一致、分片合併的排行與暴力解一致，
// 以及批次層能把被刷上排行的歌校正下來。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"os"
	"time"

	"trending-songs/internal/app"
	"trending-songs/internal/client"
	"trending-songs/internal/domain"
	"trending-songs/internal/ingest"
	"trending-songs/internal/olap"
)

func main() {
	seed := flag.Uint64("seed", 42, "random seed")
	verbose := flag.Bool("v", false, "print component logs")
	flag.Parse()

	level := slog.LevelError
	if *verbose {
		level = slog.LevelInfo
	}
	if err := run(*seed, slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))); err != nil {
		fmt.Fprintln(os.Stderr, "simulation failed:", err)
		os.Exit(1)
	}
}

var errNetwork = errors.New("network error")

// network 模擬不穩定的行動網路：有些請求根本沒送到，有些送到了但回應遺失，
// 後者會讓 client 重送同一批事件，伺服器因此收到重複的事件。
type network struct {
	svc            *ingest.Service
	user           user
	rng            *rand.Rand
	lost, failed   *int
	duplicateBatch *int
}

func (n *network) Send(_ context.Context, batch []ingest.EventInput) error {
	switch r := n.rng.Float64(); {
	case r < 0.01:
		*n.failed++
		return errNetwork
	case r < 0.03:
		n.svc.Accept(n.user.id, n.user.country, batch)
		*n.lost++
		*n.duplicateBatch += len(batch)
		return errNetwork
	}
	n.svc.Accept(n.user.id, n.user.country, batch)
	return nil
}

func run(seed uint64, logger *slog.Logger) error {
	start := time.Now()
	w := newWorkload(seed)
	tr := computeTruth(w, app.DefaultOptions(w.catalog).Stream.Rules)
	fmt.Printf("workload  %d users (%d bots) in %d countries, %d tracks, %s of listening (%s to %s UTC)\n",
		userCount, botCount, len(countries), trackCount, w.end.Sub(w.start), w.start.Format("15:04"), w.end.Format("15:04"))
	fmt.Printf("          %d sessions: %d full, %d skipped early, %d seeked to the end, %d paused, %d bot loops; %d effective plays\n",
		w.stats.sessions, w.stats.full, w.stats.skipped, w.stats.seeked, w.stats.paused, w.stats.botSessions, len(tr.facts))
	fmt.Printf("          generated in %s\n\n", time.Since(start).Round(time.Millisecond))

	opts := app.DefaultOptions(w.catalog)
	opts.Logger = logger
	opts.TopK = 100
	a := app.New(opts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		a.Run(ctx)
		close(done)
	}()
	defer func() {
		cancel()
		<-done
	}()
	time.Sleep(50 * time.Millisecond)

	feedStart := time.Now()
	sent, dup, offline := feed(ctx, a, w, seed)
	feedTook := time.Since(feedStart)
	if err := waitFor(time.Minute, a.Drained); err != nil {
		return fmt.Errorf("pipeline did not drain: %w", err)
	}
	pipelineTook := time.Since(feedStart)
	a.Refresher.Refresh()

	processed, emitted, tooLate := a.StreamStats()
	fresh := a.Freshness.Summary()
	fmt.Printf("ingest    %d events sent (%d duplicated by retries, %d buffered offline and sent late) in %s\n",
		sent, dup, offline, feedTook.Round(time.Millisecond))
	fmt.Printf("stream    %.0f events/s end to end, %d checkpoints; processor and OLAP ingest each crashed once and resumed\n",
		float64(sent)/pipelineTook.Seconds(), a.Checkpoints.Commits())
	fmt.Printf("          surviving instances read %d events and emitted %d plays after the restarts (%d events too late for the stream)\n", processed, emitted, tooLate)
	fmt.Printf("freshness event received -> counted in OLAP p50=%s p95=%s p99=%s (top-K cache refreshes every %s)\n\n",
		ms(fresh.P50), ms(fresh.P95), ms(fresh.P99), opts.RefreshEvery)

	return verify(a, w, tr, opts)
}

// feed 依虛擬時間順序執行所有動作：每位使用者的裝置各有一個 client buffer。
func feed(ctx context.Context, a *app.App, w *workload, seed uint64) (sent, dup, offline int) {
	var lost, failed int
	buffers := make([]*client.Buffer, len(w.users))
	for i, u := range w.users {
		buffers[i] = client.NewBuffer(&network{svc: a.Ingest, user: u, rng: rand.New(rand.NewPCG(seed, uint64(i)+7)),
			lost: &lost, failed: &failed, duplicateBatch: &dup}, 5)
	}
	isOffline := make([]bool, len(w.users))
	crashProcessorAt, crashIngestAt := len(w.actions)*4/10, len(w.actions)*6/10
	for i, act := range w.actions {
		switch i {
		case crashProcessorAt:
			a.CrashProcessor(1)
		case crashIngestAt:
			a.CrashOLAPIngest()
		}
		b := buffers[act.user]
		if act.online != nil {
			isOffline[act.user] = !*act.online
			b.SetOnline(ctx, *act.online)
			continue
		}
		if isOffline[act.user] {
			offline++
		}
		sent++
		b.Add(ctx, act.event)
	}
	for _, b := range buffers {
		b.SetOnline(ctx, true)
		for b.Pending() > 0 {
			b.Flush(ctx)
		}
	}
	return sent, dup, offline
}

func verify(a *app.App, w *workload, tr truth, opts app.Options) error {
	fmt.Println("verify")
	from, to := w.start, w.end.Add(time.Hour)

	// 1. 每個維度、每首歌的播放數都和正確答案完全一致。
	dims := make(map[string]struct{})
	for d := range tr.minutes {
		dims[d] = struct{}{}
	}
	for _, d := range a.OLAP.Dims() {
		dims[d] = struct{}{}
	}
	mismatched := 0
	for d := range dims {
		if !equalCounts(a.OLAP.Counts(d, olap.Hour, from, to), tr.counts(d, from, to)) {
			mismatched++
		}
	}
	check(fmt.Sprintf("stream counts equal the reference for every track in all %d dimensions (%d plays)", len(dims), len(tr.facts)), mismatched == 0)

	minuteOK := true
	for m := w.start; m.Before(w.end.Add(time.Minute)); m = m.Add(time.Minute) {
		if !equalCounts(a.OLAP.Counts(domain.DimGlobal, olap.Minute, m, m.Add(time.Minute)), tr.counts(domain.DimGlobal, m, m.Add(time.Minute))) {
			minuteOK = false
		}
	}
	check("per-minute buckets match too (late offline plays landed in the minute they happened)", minuteOK)

	// 2. 分片各自取 top K 再合併的排行，和暴力排序的結果一致。
	topOK, compared := true, 0
	for _, dim := range []string{domain.DimGlobal, "country:US", "country:TW", "genre:pop", "genre:jazz"} {
		for _, win := range []string{"1h", "1d"} {
			e, ok := a.Cache.Get(dim, win)
			if !ok || !slicesEqual(e.Tracks, topK(tr.counts(dim, e.From, e.To), opts.TopK)) {
				topOK = false
			}
			compared++
		}
	}
	check(fmt.Sprintf("top-%d from merged sub-shards equals a brute-force sort (%d rankings)", opts.TopK, compared), topOK)

	// 3. 熱門維度拆成 sub-shard 後的負載分布。
	flat := olap.NewStore(olap.Config{Shards: opts.OLAP.Shards})
	flat.Apply(0, 0, tr.facts)
	maxHot, avgHot := spread(a.OLAP.ShardLoad())
	maxFlat, avgFlat := spread(flat.ShardLoad())
	fmt.Printf("  shards    busiest shard holds %.1fx the average with hot sub-shards (%.1fx without)\n", maxHot/avgHot, maxFlat/avgFlat)

	before, _ := a.Cache.Get(domain.DimGlobal, "1d")
	fmt.Printf("\nstreaming top 5 (1d, global) before batch reconciliation:\n")
	printTop(a, before.Tracks[:5])

	// 4. 批次層重算每個整點，套用刷量規則後回頭校正 OLAP。
	fmt.Println("\nbatch reconciliation")
	var capped []domain.PlayFact
	for h := w.start; h.Before(w.end); h = h.Add(time.Hour) {
		r := a.Batch.Recompute(h)
		streamPlays := reconcile(a, r)
		fmt.Printf("  %s  stream %d plays, batch %d plays before the abuse rule, %d removed as artificial streaming\n",
			h.Format("15:04"), streamPlays, r.Plays, r.Removed)
		check(fmt.Sprintf("%s batch recount (same rules, from the raw lake) agrees with the stream", h.Format("15:04")), int64(r.Plays) == streamPlays)
	}
	capped = cappedFacts(tr.facts, opts.CapPerListener)
	ct := newTruth(capped)
	mismatched = 0
	for d := range dims {
		if !equalCounts(a.OLAP.Counts(d, olap.Hour, from, to), ct.counts(d, from, to)) {
			mismatched++
		}
	}
	check(fmt.Sprintf("after reconciliation every dimension equals the reference with the abuse rule (%d plays kept)", len(capped)), mismatched == 0)

	a.Refresher.Refresh()
	after, _ := a.Cache.Get(domain.DimGlobal, "1d")
	fmt.Printf("\ntop 5 (1d, global) after batch reconciliation:\n")
	printTop(a, after.Tracks[:5])
	check(fmt.Sprintf("the bot-promoted track fell from #%d to #%d", rankOf(before.Tracks, promoTrack), rankOf(after.Tracks, promoTrack)),
		rankOf(before.Tracks, promoTrack) == 1 && (rankOf(after.Tracks, promoTrack) == 0 || rankOf(after.Tracks, promoTrack) > 5))

	if failures > 0 {
		return fmt.Errorf("%d checks failed", failures)
	}
	return nil
}
