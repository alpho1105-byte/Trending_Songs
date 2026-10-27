package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"trending-songs/internal/api"
	"trending-songs/internal/app"
	"trending-songs/internal/auth"
	"trending-songs/internal/catalog"
)

func main() {
	addr := flag.String("addr", ":8095", "HTTP listen address")
	tracks := flag.Int("tracks", 5_000, "number of tracks in the synthetic catalog")
	dev := flag.Bool("dev", true, "enable POST /dev/token")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	if err := run(*addr, *tracks, *dev, logger); err != nil {
		logger.Error("server stopped", "err", err)
		os.Exit(1)
	}
}

func run(addr string, tracks int, dev bool, logger *slog.Logger) error {
	secret := os.Getenv("JWT_SECRET")
	if secret == "" {
		if !dev {
			return errors.New("JWT_SECRET is required when -dev=false")
		}
		secret = "dev-only-secret"
		logger.Warn("JWT_SECRET not set, using an insecure development secret")
	}

	opts := app.DefaultOptions(catalog.Generate(tracks, 1))
	opts.Logger = logger
	a := app.New(opts)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		a.Run(ctx)
	}()

	srv := &http.Server{
		Addr:              addr,
		Handler:           api.NewRouter(api.Deps{Signer: auth.NewSigner([]byte(secret)), Ingest: a.Ingest, Cache: a.Cache, Catalog: a.Catalog, Dev: dev}),
		ReadHeaderTimeout: 5 * time.Second,
	}
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	logger.Info("trending songs server listening", "addr", addr, "tracks", tracks)
	err := srv.ListenAndServe()
	stop()
	wg.Wait()
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
