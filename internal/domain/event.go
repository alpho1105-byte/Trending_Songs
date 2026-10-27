package domain

import "time"

// State 是播放器回報的狀態。
type State string

const (
	StatePlay      State = "play"
	StateHeartbeat State = "heartbeat"
	StateSeek      State = "seek"
	StatePause     State = "pause"
	StateStop      State = "stop"
)

func (s State) Valid() bool {
	switch s {
	case StatePlay, StateHeartbeat, StateSeek, StatePause, StateStop:
		return true
	}
	return false
}

// Playing 表示從這筆事件之後，歌曲處於播放中。
func (s State) Playing() bool {
	return s == StatePlay || s == StateHeartbeat || s == StateSeek
}

// Event 是裝置回報的一筆收聽事件。UserID 與 Country 由伺服器依登入身分填入，不採用 client 送來的值。
type Event struct {
	EventID    string
	SessionID  string
	UserID     string
	TrackID    string
	Country    string
	PositionMs int64
	State      State
	// ClientTs 是裝置上發生的時間（event time）。排行的時間窗以它為準，離線補送的事件也會算進正確的時段。
	ClientTs time.Time
	// ReceivedAt 是伺服器收到的時間，只用來量測處理延遲。
	ReceivedAt time.Time
}

// PlayFact 是一次有效播放：同一個 session 累積收聽達到門檻時產生，而且只產生一次。
type PlayFact struct {
	PlayID     string // 等於 session id，同一次播放重算多少次都得到同一個 ID
	UserID     string
	TrackID    string
	Country    string
	Genre      string
	At         time.Time
	ReceivedAt time.Time
}

const DimGlobal = "global"

// Dimensions 回傳一次播放要計入的所有排行維度。
func Dimensions(f PlayFact) [3]string {
	return [3]string{DimGlobal, "country:" + f.Country, "genre:" + f.Genre}
}
