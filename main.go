package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/server"
	"github.com/21S1298001/Mahiron5/tuner"
)

func main() {
	serverConfig, err := config.LoadAndParseSystemConfig("server.yml")
	if err != nil {
		slog.Error("failed to load config", "err", err)
	}

	level := slog.LevelInfo
	switch serverConfig.LogLevel {
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

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt, os.Kill)
	defer stop()

	addresses := make([]server.ListenAddress, len(serverConfig.Addresses))
	for i, addr := range serverConfig.Addresses {
		addresses[i] = server.ListenAddress{
			Http: addr.Http,
			Unix: addr.Unix,
		}
	}

	t := tuner.NewTuner("tuner")

	handler := http.NewServeMux()
	handler.HandleFunc("/", stream(t))

	slog.Info("starting servers")
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

func stream(t *tuner.Tuner) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		w.Header().Set("Content-Type", "video/mp2t")
		w.WriteHeader(200)

		t.StartStream(ctx, fmt.Sprintf("http-%s", r.RemoteAddr), w)

		slog.Info("stream ended", "remoteAddr", r.RemoteAddr)
	}
}
