package main

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/db"
	"github.com/21S1298001/Mahiron5/job"
	"github.com/21S1298001/Mahiron5/program"
	"github.com/21S1298001/Mahiron5/server"
	"github.com/21S1298001/Mahiron5/service"
	"github.com/21S1298001/Mahiron5/stream"
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

	database, err := db.Open(cfg.System.DatabasePath)
	if err != nil {
		slog.Error("failed to open database", "err", err)
		os.Exit(1)
	}

	if err := db.Migrate(context.Background(), database); err != nil {
		slog.Error("failed to run migrations", "err", err)
		os.Exit(1)
	}

	serviceStore := service.NewSQLiteStore(database)
	programStore := program.NewSQLiteStore(database)

	tm := tuner.NewTunerManager(&tuner.TunerManagerConfig{
		TunersConfig: cfg.Tuners,
	})

	sm := service.NewServiceManager(serviceStore, cfg.Channels)

	pm := program.NewProgramManager(programStore)

	stm := stream.NewStreamManager(stream.StreamManagerConfig{
		Channels:     cfg.Channels,
		EITUpdater:   pm,
		TunerManager: tm,
	})

	jm, err := job.NewManager(job.Config{MaxHistory: 100, MaxRunning: cfg.System.JobMaxRunning})
	if err != nil {
		slog.Error("failed to create job manager", "err", err)
		os.Exit(1)
	}

	job.RegisterServiceUpdater(jm, sm, stm, cfg.Channels)
	job.RegisterEPGGatherer(jm, pm, sm, stm, cfg.Channels, cfg.System.EpgRetentionDays, time.Duration(cfg.System.EpgRetrievalTime)*time.Millisecond)

	schedules := cfg.System.Jobs
	if len(schedules) == 0 {
		schedules = []config.JobScheduleConfig{
			{Key: job.ServiceUpdaterKey, Schedule: job.ServiceUpdaterDefaultSchedule},
			{Key: job.EPGGathererKey, Schedule: job.EPGGathererDefaultSchedule},
		}
		slog.Info("no job schedules in config, using defaults")
	}

	for _, js := range schedules {
		if err := jm.AddSchedule(js.Key, js.Schedule); err != nil {
			slog.Error("failed to add job schedule", "key", js.Key, "err", err)
		}
	}

	handler, err := web.NewWeb(web.WebConfig{
		ServiceManager: sm,
		ProgramManager: pm,
		StreamManager:  stm,
		TunerManager:   tm,
		JobManager:     jm,
		EpgStaleAfter:  int64(cfg.System.EpgStaleAfter),
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

	signalCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, os.Interrupt, os.Kill)
	defer stop()

	s := server.NewServer(addresses, handler)
	jm.Start()

	if err := runStartupTasks(signalCtx, sm, pm, jm, database, cfg); err != nil {
		slog.Error("startup tasks failed", "err", err)
	}

	slog.Info("starting servers")
	s.ListenAndServe()

	<-signalCtx.Done()
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(3)
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
		slog.Info("shutting down streams")
		if err := stm.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown streams", "err", err)
		}
		slog.Info("streams shut down")
		slog.Info("shutting down tuner")
		if err := tm.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown tuner", "err", err)
		}
		slog.Info("tuner shut down")
	}()
	go func() {
		defer wg.Done()
		slog.Info("shutting down job manager")
		if err := jm.Shutdown(timeoutCtx); err != nil {
			slog.Error("failed to shutdown job manager", "err", err)
		}
		slog.Info("job manager shut down")
	}()
	wg.Wait()

	slog.Info("closing database")
	if err := database.Close(); err != nil {
		slog.Error("failed to close database", "err", err)
	}
	slog.Info("database closed")

	slog.Info("exiting")
	os.Exit(0)
}

func runStartupTasks(ctx context.Context, sm *service.ServiceManager, pm *program.ProgramManager, jm *job.JobManager, database *sql.DB, cfg *config.Config) error {
	if err := sm.ReconcileChannels(ctx); err != nil {
		return fmt.Errorf("reconcile service channels: %w", err)
	}
	channelsHash := hashChannelConfig(cfg.Channels)
	storedHash, err := readMetadata(ctx, database, "channels_hash")
	if err != nil {
		slog.Warn("failed to read channels hash", "err", err)
	}
	if storedHash == "" || storedHash != channelsHash {
		slog.Info("channel config changed, will trigger service update")
		if err := writeMetadata(ctx, database, "channels_hash", channelsHash); err != nil {
			slog.Warn("failed to write channels hash", "err", err)
		}
	}

	count, err := sm.CountServices(ctx)
	if err != nil {
		return fmt.Errorf("count services: %w", err)
	}

	if count == 0 {
		slog.Info("no services cached, running initial service update")
		if _, err := jm.Enqueue(job.ServiceUpdaterKey); err != nil {
			slog.Error("failed to enqueue initial service update", "err", err)
		}
	} else if storedHash != "" && storedHash != channelsHash {
		slog.Info("channel config changed, enqueuing service update")
		if _, err := jm.Enqueue(job.ServiceUpdaterKey); err != nil {
			slog.Warn("failed to enqueue service update", "err", err)
		}
	}

	stale, _, _, err := sm.EPGSummary(ctx, int64(cfg.System.EpgStaleAfter), time.Now().UnixMilli())
	if err != nil {
		return fmt.Errorf("read EPG status: %w", err)
	}
	// EPG gathering requires a non-empty service list. If we don't have one
	// yet, the service updater above is responsible for populating it; the
	// gatherer's cron schedule (default `20,50 * * * *`) will pick up the
	// work on the next tick. Avoids a redundant gatherer run that would
	// fail with "EPG gathering requires scanned services".
	if count > 0 && stale > 0 {
		slog.Info("EPG is stale, enqueuing gatherer", "staleServices", stale)
		if _, err := jm.Enqueue(job.EPGGathererKey); err != nil && !errors.Is(err, job.ErrJobAlreadyRunning) {
			slog.Warn("failed to enqueue startup EPG gathering", "err", err)
		}
	}

	if cfg.System.EpgRetentionDays > 0 {
		cutoff := time.Now().Add(-time.Duration(cfg.System.EpgRetentionDays) * 24 * time.Hour).UnixMilli()
		if err := pm.DeleteEndedBefore(ctx, cutoff); err != nil {
			slog.Warn("failed to clean up old EPG data", "err", err)
		} else {
			slog.Info("cleaned up EPG data", "cutoffDays", cfg.System.EpgRetentionDays)
		}
	}

	return nil
}

func hashChannelConfig(channels config.ChannelsConfig) string {
	data, err := json.Marshal(channels)
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%x", sha256.Sum256(data))
}

func readMetadata(ctx context.Context, db *sql.DB, key string) (string, error) {
	var value string
	err := db.QueryRowContext(ctx, "SELECT value FROM metadata WHERE key = ?", key).Scan(&value)
	if err == sql.ErrNoRows {
		return "", nil
	}
	return value, err
}

func writeMetadata(ctx context.Context, db *sql.DB, key, value string) error {
	_, err := db.ExecContext(ctx, "INSERT OR REPLACE INTO metadata (key, value) VALUES (?, ?)", key, value)
	return err
}
