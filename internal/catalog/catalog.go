package catalog

import (
	"fmt"
	"math/rand/v2"
)

var Genres = []string{"pop", "rock", "hiphop", "electronic", "jazz", "classical", "kpop", "latin"}

type Track struct {
	ID         string
	Title      string
	Genre      string
	DurationMs int64
}

// Catalog 是歌曲目錄，stream processor 用它替播放補上曲風。
type Catalog struct {
	tracks map[string]Track
	ids    []string
}

func New(tracks []Track) *Catalog {
	c := &Catalog{tracks: make(map[string]Track, len(tracks))}
	for _, t := range tracks {
		c.tracks[t.ID] = t
		c.ids = append(c.ids, t.ID)
	}
	return c
}

// Generate 產生 n 首合成的歌曲，長度介於 2 到 5 分鐘。
func Generate(n int, seed uint64) *Catalog {
	rng := rand.New(rand.NewPCG(seed, 5))
	tracks := make([]Track, n)
	for i := range tracks {
		tracks[i] = Track{
			ID:         fmt.Sprintf("trk-%05d", i),
			Title:      fmt.Sprintf("Track %05d", i),
			Genre:      Genres[rng.IntN(len(Genres))],
			DurationMs: int64(120_000 + rng.IntN(180_000)),
		}
	}
	return New(tracks)
}

func (c *Catalog) Get(id string) (Track, bool) {
	t, ok := c.tracks[id]
	return t, ok
}

func (c *Catalog) Add(t Track) {
	if _, ok := c.tracks[t.ID]; !ok {
		c.ids = append(c.ids, t.ID)
	}
	c.tracks[t.ID] = t
}

func (c *Catalog) IDs() []string {
	return c.ids
}
