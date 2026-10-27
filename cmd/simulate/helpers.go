package main

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"trending-songs/internal/app"
	"trending-songs/internal/batch"
	"trending-songs/internal/domain"
	"trending-songs/internal/olap"
	"trending-songs/internal/playcount"
)

var failures int

func check(desc string, ok bool) {
	mark := "ok  "
	if !ok {
		mark = "FAIL"
		failures++
	}
	fmt.Printf("  [%s] %s\n", mark, desc)
}

func waitFor(timeout time.Duration, cond func() bool) error {
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			return errors.New("timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil
}

func ms(d time.Duration) string {
	if d >= time.Second {
		return fmt.Sprintf("%.2fs", d.Seconds())
	}
	return fmt.Sprintf("%.1fms", float64(d.Microseconds())/1000)
}

func slicesEqual(a, b []olap.TrackCount) bool {
	return slices.Equal(a, b)
}

func spread(load []int64) (maxLoad, avg float64) {
	var total int64
	for _, n := range load {
		total += n
		maxLoad = max(maxLoad, float64(n))
	}
	return maxLoad, float64(total) / float64(len(load))
}

func printTop(a *app.App, tracks []olap.TrackCount) {
	for i, t := range tracks {
		meta, _ := a.Catalog.Get(t.TrackID)
		fmt.Printf("  #%d  %-12s %-11s %5d plays\n", i+1, meta.Title, meta.Genre, t.Plays)
	}
}

func reconcile(a *app.App, r batch.Result) int64 {
	return batch.Reconcile(a.OLAP, r)
}

func cappedFacts(facts []domain.PlayFact, limit int) []domain.PlayFact {
	kept, _ := playcount.CapPerListener(facts, limit)
	return kept
}
