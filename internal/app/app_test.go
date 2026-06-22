package app

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/db"
	"github.com/21S1298001/Mahiron5/internal/job"
	"github.com/21S1298001/Mahiron5/internal/observability"
)

func TestBuildRuntimeWiresCurrentApplication(t *testing.T) {
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	obs := observability.Setup(t.Context(), config.ObservabilityConfig{}, nil)
	cfg := &config.Config{System: &config.SystemConfig{
		Addresses:          []config.ServerAddress{{Http: "127.0.0.1:0"}},
		JobMaxRunning:      1,
		EpgRetrievalTime:   5_000,
		EpgStaleAfter:      7_200_000,
		LogoGatherDuration: 86_400_000,
	}}

	runtime, message, err := buildRuntime(cfg, database, obs)
	if err != nil {
		t.Fatalf("buildRuntime() message=%q err=%v", message, err)
	}
	if runtime.database == nil || runtime.jobs == nil || runtime.programs == nil || runtime.server == nil ||
		runtime.services == nil || runtime.streams == nil || runtime.tuners == nil {
		t.Fatalf("incomplete runtime: %#v", runtime)
	}

	runtime.shutdown()
	if err := database.PingContext(t.Context()); err == nil {
		t.Fatal("runtime shutdown left the database open")
	}
}

func TestStartupQueuePolicyUsesCurrentState(t *testing.T) {
	tests := []struct {
		name         string
		serviceCount int
		state        channelConfigState
		stale        int
		wantService  bool
		wantEPG      bool
	}{
		{name: "empty cache scans services", serviceCount: 0, wantService: true},
		{name: "changed persisted channels rescan", serviceCount: 2, state: channelConfigState{storedHash: "old", currentHash: "new"}, wantService: true},
		{name: "stale EPG gathers", serviceCount: 2, state: channelConfigState{storedHash: "same", currentHash: "same"}, stale: 1, wantEPG: true},
		{name: "fresh populated cache does nothing", serviceCount: 2, state: channelConfigState{storedHash: "same", currentHash: "same"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr, err := job.NewManager(job.Config{MaxHistory: 10, MaxRunning: 2})
			if err != nil {
				t.Fatal(err)
			}
			release := make(chan struct{})
			for _, key := range []string{job.ServiceUpdaterKey, job.EPGGathererKey} {
				mgr.Register(job.JobDefinition{Key: key, Handler: func(ctx context.Context) error {
					select {
					case <-release:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				}})
			}

			enqueueStartupServiceUpdate(mgr, tt.serviceCount, tt.state)
			enqueueStartupEPGGather(mgr, tt.serviceCount, tt.stale)
			active := mgr.GetActiveJobKeysByPrefix("")
			if got := containsKey(active, job.ServiceUpdaterKey); got != tt.wantService {
				t.Errorf("service updater queued=%v, want %v; active=%v", got, tt.wantService, active)
			}
			if got := containsKey(active, job.EPGGathererKey); got != tt.wantEPG {
				t.Errorf("EPG gatherer queued=%v, want %v; active=%v", got, tt.wantEPG, active)
			}

			close(release)
			ctx, cancel := context.WithTimeout(t.Context(), time.Second)
			defer cancel()
			if err := mgr.Shutdown(ctx); err != nil && !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
}

func TestHashChannelConfigIsStableAndSensitiveToRoutes(t *testing.T) {
	base := config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27"}}
	if hashChannelConfig(base) != hashChannelConfig(append(config.ChannelsConfig(nil), base...)) {
		t.Fatal("equivalent channel configurations produced different hashes")
	}
	changed := config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27", Routes: []config.ChannelRouteConfig{{Id: "catv", Type: "CATV", Channel: "C27"}}}}
	if hashChannelConfig(base) == hashChannelConfig(changed) {
		t.Fatal("route change was not reflected in channel configuration hash")
	}
}

func containsKey(keys []string, want string) bool {
	for _, key := range keys {
		if key == want {
			return true
		}
	}
	return false
}
