package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"trending-songs/internal/app"
	"trending-songs/internal/auth"
	"trending-songs/internal/catalog"
	"trending-songs/internal/ingest"
)

type server struct {
	t       *testing.T
	app     *app.App
	handler http.Handler
	signer  *auth.Signer
}

func newServer(t *testing.T) *server {
	t.Helper()
	opts := app.DefaultOptions(catalog.Generate(20, 1))
	opts.Logger = slog.New(slog.DiscardHandler)
	opts.RefreshEvery = time.Hour
	a := app.New(opts)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		a.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	signer := auth.NewSigner([]byte("secret"))
	return &server{t: t, app: a, signer: signer,
		handler: NewRouter(Deps{Signer: signer, Ingest: a.Ingest, Cache: a.Cache, Catalog: a.Catalog, Dev: true})}
}

func (s *server) do(method, path, body, user string) (int, map[string]any) {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if user != "" {
		tok, _ := s.signer.Issue(auth.Identity{UserID: user, Country: "TW"}, time.Hour)
		req.Header.Set("Authorization", "Bearer "+tok)
	}
	rec := httptest.NewRecorder()
	s.handler.ServeHTTP(rec, req)
	var out map[string]any
	json.NewDecoder(rec.Body).Decode(&out)
	return rec.Code, out
}

func listenBody(session, track string, start time.Time) string {
	var events []ingest.EventInput
	for at := time.Duration(0); at <= 40*time.Second; at += 5 * time.Second {
		state := "heartbeat"
		if at == 0 {
			state = "play"
		}
		events = append(events, ingest.EventInput{EventID: fmt.Sprintf("%s-%d", session, len(events)), SessionID: session,
			TrackID: track, PositionMs: at.Milliseconds(), State: state, ClientTs: start.Add(at).UnixMilli()})
	}
	b, _ := json.Marshal(map[string]any{"events": events})
	return string(b)
}

func TestEventsRequireAValidTokenAndBody(t *testing.T) {
	s := newServer(t)
	body := listenBody("s1", "trk-00001", time.Now().Add(-time.Minute))
	if code, _ := s.do("POST", "/tracks/events", body, ""); code != http.StatusUnauthorized {
		t.Fatalf("no token: %d", code)
	}
	if code, _ := s.do("POST", "/tracks/events", `{"events":[]}`, "alice"); code != http.StatusBadRequest {
		t.Fatalf("empty batch: %d", code)
	}
	code, out := s.do("POST", "/tracks/events", body, "alice")
	if code != http.StatusAccepted || out["accepted"] != float64(9) || out["rejected"] != float64(0) {
		t.Fatalf("accept: %d %v", code, out)
	}
}

func TestTopTracksServesTheRefreshedRanking(t *testing.T) {
	s := newServer(t)
	start := time.Now().Add(-2 * time.Minute)
	for i, track := range []string{"trk-00003", "trk-00003", "trk-00007"} {
		if code, _ := s.do("POST", "/tracks/events", listenBody(fmt.Sprintf("s%d", i), track, start), "alice"); code != http.StatusAccepted {
			t.Fatalf("post %d: %d", i, code)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for !s.app.Drained() {
		if time.Now().After(deadline) {
			t.Fatal("pipeline did not drain")
		}
		time.Sleep(10 * time.Millisecond)
	}
	s.app.Refresher.Refresh()

	code, out := s.do("GET", "/tracks/top_tracks?dim=country:TW&window=1h&num_tracks=1", "", "")
	tracks, _ := out["tracks"].([]any)
	if code != http.StatusOK || len(tracks) != 1 {
		t.Fatalf("top tracks: %d %v", code, out)
	}
	if top := tracks[0].(map[string]any); top["trackId"] != "trk-00003" || top["plays"] != float64(2) || top["rank"] != float64(1) {
		t.Fatalf("top = %v", top)
	}

	for path, want := range map[string]int{
		"/tracks/top_tracks?dim=country:ZZ":       http.StatusNotFound,
		"/tracks/top_tracks?window=7d":            http.StatusBadRequest,
		"/tracks/top_tracks?num_tracks=0":         http.StatusBadRequest,
		"/tracks/top_tracks?dim=global&window=1d": http.StatusOK,
	} {
		if code, _ := s.do("GET", path, "", ""); code != want {
			t.Errorf("%s: %d, want %d", path, code, want)
		}
	}
}
