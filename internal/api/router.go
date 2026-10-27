package api

import (
	"encoding/json"
	"net/http"
	"strconv"
	"strings"
	"time"

	"trending-songs/internal/auth"
	"trending-songs/internal/catalog"
	"trending-songs/internal/ingest"
	"trending-songs/internal/topk"
)

const maxEventsPerRequest = 500

type Deps struct {
	Signer  *auth.Signer
	Ingest  *ingest.Service
	Cache   *topk.Cache
	Catalog *catalog.Catalog
	// Dev 為 true 時註冊 /dev/token，方便本機簽發測試用的 token。
	Dev bool
}

func NewRouter(d Deps) http.Handler {
	mux := http.NewServeMux()
	h := &handler{d: d}
	mux.HandleFunc("POST /tracks/events", h.events)
	mux.HandleFunc("GET /tracks/top_tracks", h.topTracks)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	if d.Dev {
		mux.HandleFunc("POST /dev/token", h.token)
	}
	return mux
}

type handler struct {
	d Deps
}

// POST /tracks/events：裝置批次送出收聽事件。收下就回應，計數由下游非同步完成。
func (h *handler) events(w http.ResponseWriter, r *http.Request) {
	token, ok := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	if !ok {
		writeError(w, http.StatusUnauthorized, "missing bearer token")
		return
	}
	id, err := h.d.Signer.Verify(token)
	if err != nil {
		writeError(w, http.StatusUnauthorized, err.Error())
		return
	}
	var req struct {
		Events []ingest.EventInput `json:"events"`
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<20))
	if err := dec.Decode(&req); err != nil || len(req.Events) == 0 || len(req.Events) > maxEventsPerRequest {
		writeError(w, http.StatusBadRequest, "events must contain 1 to 500 items")
		return
	}
	accepted, rejected := h.d.Ingest.Accept(id.UserID, id.Country, req.Events)
	writeJSON(w, http.StatusAccepted, map[string]int{"accepted": accepted, "rejected": rejected})
}

// GET /tracks/top_tracks?dim=country:TW&num_tracks=100&window=1h
// dim 可以是 global、country:<代碼> 或 genre:<曲風>；window 是 1h 或 1d。
func (h *handler) topTracks(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	dim := q.Get("dim")
	if dim == "" {
		dim = "global"
	}
	window := q.Get("window")
	if window == "" {
		window = "1h"
	}
	if _, ok := topk.WindowByName(window); !ok {
		writeError(w, http.StatusBadRequest, "window must be 1h or 1d")
		return
	}
	n := 100
	if s := q.Get("num_tracks"); s != "" {
		v, err := strconv.Atoi(s)
		if err != nil || v < 1 || v > 100 {
			writeError(w, http.StatusBadRequest, "num_tracks must be between 1 and 100")
			return
		}
		n = v
	}
	entry, ok := h.d.Cache.Get(dim, window)
	if !ok {
		writeError(w, http.StatusNotFound, "no plays recorded for this dimension yet")
		return
	}

	tracks := entry.Tracks[:min(n, len(entry.Tracks))]
	out := make([]map[string]any, len(tracks))
	for i, t := range tracks {
		item := map[string]any{"rank": i + 1, "trackId": t.TrackID, "plays": t.Plays}
		if meta, found := h.d.Catalog.Get(t.TrackID); found {
			item["title"], item["genre"] = meta.Title, meta.Genre
		}
		out[i] = item
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"dim": dim, "window": window, "from": entry.From.UnixMilli(), "to": entry.To.UnixMilli(),
		"computedAt": entry.ComputedAt.UnixMilli(), "tracks": out,
	})
}

func (h *handler) token(w http.ResponseWriter, r *http.Request) {
	var req struct {
		UserID  string `json:"userId"`
		Country string `json:"country"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.UserID == "" || len(req.Country) != 2 {
		writeError(w, http.StatusBadRequest, "userId and a two-letter country are required")
		return
	}
	token, _ := h.d.Signer.Issue(auth.Identity{UserID: req.UserID, Country: strings.ToUpper(req.Country)}, 24*time.Hour)
	writeJSON(w, http.StatusOK, map[string]string{"token": token})
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, msg string) {
	writeJSON(w, status, map[string]string{"error": msg})
}
