package client

import (
	"context"

	"trending-songs/internal/ingest"
)

// Sender 把一批事件送到伺服器。回傳錯誤時，這批事件「可能」其實已經送達（例如回應在路上遺失），
// 所以重送會造成重複事件；伺服器端必須能用 event_id 去重。
type Sender interface {
	Send(ctx context.Context, batch []ingest.EventInput) error
}

// Buffer 是裝置端的事件緩衝：累積到 batchSize 筆才送一次，節省網路請求；
// 離線時先存在本機（正式 app 會用 SQLite 之類的本機資料庫），恢復連線後依原本的順序補送。
// 前一批沒送成功之前不會送下一批，同一個 session 的事件順序因此不會亂掉。
type Buffer struct {
	sender    Sender
	batchSize int
	online    bool
	pending   []ingest.EventInput
}

func NewBuffer(sender Sender, batchSize int) *Buffer {
	return &Buffer{sender: sender, batchSize: batchSize, online: true}
}

func (b *Buffer) Add(ctx context.Context, e ingest.EventInput) {
	b.pending = append(b.pending, e)
	if b.online && len(b.pending) >= b.batchSize {
		b.Flush(ctx)
	}
}

// SetOnline 切換連線狀態；恢復連線時立刻補送離線期間累積的事件。
func (b *Buffer) SetOnline(ctx context.Context, online bool) {
	b.online = online
	if online {
		b.Flush(ctx)
	}
}

// Flush 依序送出所有待送事件，遇到錯誤就停下，留到下次重試。
func (b *Buffer) Flush(ctx context.Context) error {
	for len(b.pending) > 0 && b.online {
		n := min(b.batchSize, len(b.pending))
		if err := b.sender.Send(ctx, b.pending[:n]); err != nil {
			return err
		}
		b.pending = b.pending[n:]
	}
	return nil
}

func (b *Buffer) Pending() int {
	return len(b.pending)
}
