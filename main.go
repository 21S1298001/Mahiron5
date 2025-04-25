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
	"github.com/21S1298001/Mahiron5/service"
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

	tm := tuner.NewTunerManager(&tuner.TunerManagerConfig{
		TunersConfig: cfg.Tuners,
	})

	sm := service.NewServiceManager(&service.ServiceManagerConfig{
		Channels: cfg.Channels,
	})

	handler, err := web.NewWeb(web.WebConfig{
		ServiceManager: sm,
		TunerManager:   tm,
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

	go func() {
		slog.Info("starting service scan", "channels", len(cfg.Channels))
		for _, channel := range cfg.Channels {
			t := ""
			tg := channel.TunerGroups
			if len(tg) == 0 {
				t = channel.Type
			} else {
				t = tg[0]
			}
			slog.Info("processing channel", "group", t, "channel", channel.Channel)
			err := sm.ScanServices(signalCtx, tm.GetTunerByGroup(t), channel.Type, channel.Channel)
			if err != nil {
				slog.Error("failed to scan services", "group", t, "channel", channel.Channel, "err", err)
				continue
			}
			slog.Info("scanned services", "group", t, "channel", channel.Channel)
		}
		slog.Info("completed scanning services", "discovered", sm.CountServices())
	}()

	<-signalCtx.Done()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		slog.Info("shutting down servers")
		if err := s.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown servers", "err", err)
		}
		slog.Info("servers shut down")
	}()
	go func() {
		defer wg.Done()
		slog.Info("shutting down tuner")
		if err := tm.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown tuner", "err", err)
		}
		slog.Info("tuner shut down")
	}()
	wg.Wait()

	slog.Info("exiting")
	os.Exit(0)
}
