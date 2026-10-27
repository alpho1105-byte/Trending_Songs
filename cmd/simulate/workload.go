package main

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"sort"
	"time"

	"trending-songs/internal/catalog"
	"trending-songs/internal/domain"
	"trending-songs/internal/ingest"
)

const (
	trackCount  = 5_000
	userCount   = 3_000
	botCount    = 40
	promoTrack  = "trk-promo"
	heartbeat   = 5 * time.Second
	offlineRate = 0.05
)

var countries = []struct {
	code   string
	weight float64
}{
	{"US", 35}, {"BR", 12}, {"GB", 8}, {"DE", 8}, {"JP", 8}, {"TW", 6}, {"KR", 6}, {"MX", 6}, {"FR", 6}, {"IN", 5},
}

type user struct {
	id      string
	country string
	bot     bool
}

// action 是虛擬時間軸上的一個動作：送出一筆事件，或切換連線狀態。
type action struct {
	at     time.Time
	user   int
	online *bool
	event  ingest.EventInput
}

type workload struct {
	catalog *catalog.Catalog
	users   []user
	actions []action
	// clean 是每個 session 原始、無重複、依序的事件，用來計算正確答案。
	clean map[string][]domain.Event
	start time.Time
	end   time.Time
	stats struct{ sessions, full, skipped, seeked, paused, botSessions, offlineUsers int }
}

// popularity 依 Zipf 分布挑歌；每個國家另有一批在地熱門歌曲。
type popularity struct {
	ids   []string
	cum   []float64
	local map[string][]string
}

func newPopularity(c *catalog.Catalog, rng *rand.Rand) *popularity {
	p := &popularity{ids: slices.Clone(c.IDs()), local: make(map[string][]string)}
	rng.Shuffle(len(p.ids), func(i, j int) { p.ids[i], p.ids[j] = p.ids[j], p.ids[i] })
	var total float64
	for i := range p.ids {
		total += 1 / float64(i+1)
		p.cum = append(p.cum, total)
	}
	for i := range p.cum {
		p.cum[i] /= total
	}
	for _, ct := range countries {
		for range 20 {
			p.local[ct.code] = append(p.local[ct.code], p.ids[200+rng.IntN(2_000)])
		}
	}
	return p
}

func (p *popularity) pick(rng *rand.Rand, country string) string {
	if rng.Float64() < 0.3 {
		local := p.local[country]
		return local[min(len(local)-1, int(float64(len(local))*rng.Float64()*rng.Float64()))]
	}
	return p.ids[sort.SearchFloat64s(p.cum, rng.Float64())]
}

func newWorkload(seed uint64) *workload {
	rng := rand.New(rand.NewPCG(seed, 1))
	c := catalog.Generate(trackCount, seed)
	c.Add(catalog.Track{ID: promoTrack, Title: "Promo Track", Genre: "pop", DurationMs: 180_000})
	pop := newPopularity(c, rng)

	// 兩個完整的小時，結束在目前時間的一小時前，事件時間不會落在未來。
	end := time.Now().UTC().Truncate(time.Hour).Add(-time.Hour)
	w := &workload{catalog: c, clean: make(map[string][]domain.Event), start: end.Add(-2 * time.Hour), end: end}

	var total float64
	for _, ct := range countries {
		total += ct.weight
	}
	for i := range userCount {
		r, country := rng.Float64()*total, countries[0].code
		for _, ct := range countries {
			if r < ct.weight {
				country = ct.code
				break
			}
			r -= ct.weight
		}
		w.users = append(w.users, user{id: fmt.Sprintf("user-%04d", i), country: country, bot: i < botCount})
	}

	for ui, u := range w.users {
		w.generateUser(rng, ui, u, pop)
	}
	slices.SortStableFunc(w.actions, func(a, b action) int { return a.at.Compare(b.at) })
	return w
}

func (w *workload) generateUser(rng *rand.Rand, ui int, u user, pop *popularity) {
	t, stop := w.start, w.end
	if !u.bot {
		if rng.Float64() < 0.3 {
			return
		}
		t = w.start.Add(time.Duration(rng.Int64N(int64(30 * time.Minute))))
		stop = t.Add(30*time.Minute + time.Duration(rng.Int64N(int64(90*time.Minute))))
		if stop.After(w.end) {
			stop = w.end
		}
		if rng.Float64() < offlineRate {
			off := t.Add(time.Duration(rng.Int64N(int64(stop.Sub(t)))))
			back := off.Add(5*time.Minute + time.Duration(rng.Int64N(int64(10*time.Minute))))
			f, on := false, true
			w.actions = append(w.actions, action{at: off, user: ui, online: &f}, action{at: back, user: ui, online: &on})
			w.stats.offlineUsers++
		}
	}

	for n := 0; t.Before(stop); n++ {
		sid := fmt.Sprintf("s-%d-%d", ui, n)
		track := promoTrack
		if !u.bot {
			track = pop.pick(rng, u.country)
		}
		meta, _ := w.catalog.Get(track)
		var plan []step
		switch r := rng.Float64(); {
		case u.bot:
			// 機器人帳號：每次只聽 35 秒剛好過門檻，立刻重播同一首。
			plan = listen(0, 35*time.Second)
			w.stats.botSessions++
		case r < 0.60:
			plan = listen(0, time.Duration(meta.DurationMs)*time.Millisecond)
			w.stats.full++
		case r < 0.85:
			plan = listen(0, time.Duration(3+rng.IntN(23))*time.Second)
			w.stats.skipped++
		case r < 0.93:
			// 聽 3 秒就快轉到最後 5 秒：事件數不少，但實際只聽了 8 秒。
			plan = append(listen(0, 3*time.Second)[:1], step{at: 3 * time.Second, pos: meta.DurationMs - 5_000, state: domain.StateSeek})
			plan = append(plan, listenFrom(3*time.Second, meta.DurationMs-5_000, 5*time.Second)[1:]...)
			w.stats.seeked++
		default:
			// 聽 20 秒、暫停 30 秒、再聽 30 秒。
			plan = append(listen(0, 20*time.Second)[:4], step{at: 20 * time.Second, pos: 20_000, state: domain.StatePause},
				step{at: 50 * time.Second, pos: 20_000, state: domain.StatePlay})
			plan = append(plan, listenFrom(50*time.Second, 20_000, 30*time.Second)[1:]...)
			w.stats.paused++
		}
		w.stats.sessions++
		for i, s := range plan {
			at := t.Add(s.at)
			in := ingest.EventInput{EventID: fmt.Sprintf("%s-%d", sid, i), SessionID: sid, TrackID: track,
				PositionMs: s.pos, State: string(s.state), ClientTs: at.UnixMilli()}
			w.actions = append(w.actions, action{at: at, user: ui, event: in})
			w.clean[sid] = append(w.clean[sid], domain.Event{EventID: in.EventID, SessionID: sid, UserID: u.id,
				TrackID: track, Country: u.country, PositionMs: s.pos, State: s.state, ClientTs: time.UnixMilli(in.ClientTs)})
		}
		t = t.Add(plan[len(plan)-1].at + time.Duration(rng.IntN(3))*time.Second)
		if !u.bot && rng.Float64() < 0.05 {
			t = t.Add(time.Duration(1+rng.IntN(10)) * time.Minute)
		}
	}
}

type step struct {
	at    time.Duration
	pos   int64
	state domain.State
}

// listen 從位置 0 播放 d 這麼久：開頭一筆 play、每 5 秒一筆 heartbeat、結尾一筆 stop。
func listen(at time.Duration, d time.Duration) []step {
	return listenFrom(at, 0, d)
}

func listenFrom(at time.Duration, pos int64, d time.Duration) []step {
	steps := []step{{at: at, pos: pos, state: domain.StatePlay}}
	for e := heartbeat; e < d; e += heartbeat {
		steps = append(steps, step{at: at + e, pos: pos + e.Milliseconds(), state: domain.StateHeartbeat})
	}
	return append(steps, step{at: at + d, pos: pos + d.Milliseconds(), state: domain.StateStop})
}
