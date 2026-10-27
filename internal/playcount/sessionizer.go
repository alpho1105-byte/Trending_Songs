package playcount

import (
	"maps"
	"time"

	"trending-songs/internal/domain"
)

// Rules 定義什麼算一次有效播放。
type Rules struct {
	// Threshold 是一次播放至少要實際聽多久。
	Threshold time.Duration
	// MaxGap 是兩筆事件之間的最大間隔，超過就視為中斷（例如裝置斷線），中間不算收聽時間。
	MaxGap time.Duration
}

func DefaultRules() Rules {
	return Rules{Threshold: 30 * time.Second, MaxGap: 15 * time.Second}
}

type session struct {
	seen          map[string]struct{}
	lastTs        time.Time
	lastPos       int64
	hasLast       bool
	playing       bool
	accumulatedMs int64
	counted       bool
}

// Sessionizer 依 session 累積實際收聽的時間，達到門檻時產生一次 PlayFact。
// 串流處理與批次重算共用這份邏輯，兩邊的結果才能互相比對。
//
// 單純計算事件數一定會高估：播放後立刻快轉到結尾再切歌，也會送出好幾筆事件。
// 這裡只累積「播放中」兩筆事件之間的真實時間：
// 播放位置前進的距離與經過的時間取較小值，快轉跳過的部分就不算。
type Sessionizer struct {
	rules    Rules
	sessions map[string]*session
	// counted 是已經計過、完整狀態已被清掉的 session，只留最後一筆事件的時間。
	// client 可能在很久之後才補送卡在 buffer 裡的事件；沒有這份紀錄，
	// 那些事件會被當成新的 session 從頭累積，同一次播放就被算兩次。
	counted map[string]time.Time
}

func NewSessionizer(rules Rules) *Sessionizer {
	return &Sessionizer{rules: rules, sessions: make(map[string]*session), counted: make(map[string]time.Time)}
}

func (s *Sessionizer) Process(e domain.Event) (domain.PlayFact, bool) {
	if _, done := s.counted[e.SessionID]; done {
		return domain.PlayFact{}, false
	}
	st, ok := s.sessions[e.SessionID]
	if !ok {
		st = &session{seen: make(map[string]struct{})}
		s.sessions[e.SessionID] = st
	}
	// 用 event_id 去重：client 重試會送出同樣的事件。
	if _, dup := st.seen[e.EventID]; dup {
		return domain.PlayFact{}, false
	}
	st.seen[e.EventID] = struct{}{}
	if st.hasLast && e.ClientTs.Before(st.lastTs) {
		return domain.PlayFact{}, false
	}

	if st.hasLast && st.playing {
		dt := e.ClientTs.Sub(st.lastTs)
		dp := e.PositionMs - st.lastPos
		if dt <= s.rules.MaxGap && dp > 0 {
			st.accumulatedMs += min(dp, dt.Milliseconds())
		}
	}
	st.playing = e.State.Playing()
	st.lastTs, st.lastPos, st.hasLast = e.ClientTs, e.PositionMs, true

	if !st.counted && st.accumulatedMs >= s.rules.Threshold.Milliseconds() {
		st.counted = true
		return domain.PlayFact{
			PlayID: e.SessionID, UserID: e.UserID, TrackID: e.TrackID, Country: e.Country,
			At: e.ClientTs, ReceivedAt: e.ReceivedAt,
		}, true
	}
	return domain.PlayFact{}, false
}

// Evict 讓狀態大小維持有界：最後一筆事件早於 stateBefore 的 session 清掉完整狀態
// （已計過的只留一筆小紀錄），早於 countedBefore 的紀錄再整個移除。
func (s *Sessionizer) Evict(stateBefore, countedBefore time.Time) {
	for id, st := range s.sessions {
		if st.lastTs.Before(stateBefore) {
			if st.counted {
				s.counted[id] = st.lastTs
			}
			delete(s.sessions, id)
		}
	}
	for id, ts := range s.counted {
		if ts.Before(countedBefore) {
			delete(s.counted, id)
		}
	}
}

// Snapshot 回傳完整的深層複本，作為 checkpoint 的狀態。
func (s *Sessionizer) Snapshot() *Sessionizer {
	c := &Sessionizer{rules: s.rules, sessions: make(map[string]*session, len(s.sessions)), counted: maps.Clone(s.counted)}
	for id, st := range s.sessions {
		cp := *st
		cp.seen = maps.Clone(st.seen)
		c.sessions[id] = &cp
	}
	return c
}
