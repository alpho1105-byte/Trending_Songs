package ingest

import (
	"time"

	"trending-songs/internal/catalog"
	"trending-songs/internal/domain"
	"trending-songs/internal/topic"
)

// EventInput 是裝置送來的一筆事件。
type EventInput struct {
	EventID    string `json:"event_id"`
	SessionID  string `json:"session_id"`
	TrackID    string `json:"track_id"`
	PositionMs int64  `json:"position_ms"`
	State      string `json:"state"`
	ClientTs   int64  `json:"client_ts"` // unix 毫秒
}

// Service 是收集播放事件的伺服器。它沒有狀態：驗證後依 session_id 分區寫進事件 topic 就回應，
// 可以任意水平擴充。去重與計算有效播放都交給下游的 stream processor。
type Service struct {
	events  *topic.Topic[domain.Event]
	catalog *catalog.Catalog
	now     func() time.Time
}

func NewService(events *topic.Topic[domain.Event], c *catalog.Catalog) *Service {
	return &Service{events: events, catalog: c, now: time.Now}
}

// Accept 收下一批事件，回傳收下與拒絕的筆數。格式錯誤的事件個別丟棄，不影響同一批的其他事件。
// userID 與 country 來自登入身分，不採用 client 自己宣稱的值。
func (s *Service) Accept(userID, country string, in []EventInput) (accepted, rejected int) {
	now := s.now()
	byPart := make(map[int][]domain.Event)
	for _, e := range in {
		if !s.valid(e, now) {
			rejected++
			continue
		}
		ev := domain.Event{
			EventID: e.EventID, SessionID: e.SessionID, UserID: userID, TrackID: e.TrackID, Country: country,
			PositionMs: e.PositionMs, State: domain.State(e.State), ClientTs: time.UnixMilli(e.ClientTs), ReceivedAt: now,
		}
		// 以 session_id 分區：同一次播放的事件一定由同一個 processor 依序處理。
		p := s.events.PartitionFor(e.SessionID)
		byPart[p] = append(byPart[p], ev)
		accepted++
	}
	for p, evs := range byPart {
		s.events.Append(p, evs...)
	}
	return accepted, rejected
}

func (s *Service) valid(e EventInput, now time.Time) bool {
	if e.EventID == "" || len(e.EventID) > 64 || e.SessionID == "" || len(e.SessionID) > 64 || e.PositionMs < 0 {
		return false
	}
	if !domain.State(e.State).Valid() {
		return false
	}
	if _, ok := s.catalog.Get(e.TrackID); !ok {
		return false
	}
	// 離線補送的事件可能晚到好幾天，但不能來自未來。
	ts := time.UnixMilli(e.ClientTs)
	return !ts.After(now.Add(5*time.Minute)) && ts.After(now.Add(-7*24*time.Hour))
}
