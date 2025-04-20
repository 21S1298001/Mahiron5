package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/server"
	"github.com/21S1298001/Mahiron5/tuner"
	"github.com/21S1298001/Mahiron5/web"
)

func main() {
	cfg, err := config.LoadAndParseConfig()
	if err != nil {
		slog.Error("failed to load config", "err", err)
		os.Exit(1)
	}

	level := slog.LevelInfo
	switch cfg.System.LogLevel {
	case "debug":
		level = slog.LevelDebug
	case "info":
		level = slog.LevelInfo
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	h := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	slog.SetDefault(slog.New(h))

	t := tuner.NewTuner("tuner")

	handler, err := web.NewWeb(web.WebConfig{
		Tuner: t,
	})
	if err != nil {
		slog.Error("failed to create web handler", "err", err)
		os.Exit(1)
	}

	addresses := make([]server.ListenAddress, len(cfg.System.Addresses))
	for i, addr := range cfg.System.Addresses {
		addresses[i] = server.ListenAddress{
			Http: addr.Http,
			Unix: addr.Unix,
		}
	}

	slog.Info("starting servers")
	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt, os.Kill)
	defer stop()

	s := server.NewServer(addresses, handler)
	s.ListenAndServe()

	<-signalCtx.Done()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("shutting down servers")
		s.Shutdown(timeoutCtx)
		slog.Info("servers shut down")
	}()
	wg.Add(1)
	go func() {
		defer wg.Done()
		slog.Info("shutting down tuner")
		t.Shutdown(timeoutCtx)
		slog.Info("tuner shut down")
	}()
	wg.Wait()

	slog.Info("exiting")
	os.Exit(0)
}
