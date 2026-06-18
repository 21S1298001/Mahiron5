package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/db"
	"github.com/21S1298001/Mahiron5/program"
	"github.com/21S1298001/Mahiron5/service"
	"github.com/21S1298001/Mahiron5/stream"
	"github.com/21S1298001/Mahiron5/tuner"
)

type noTunerManager struct{}

func (noTunerManager) NewDeviceByType(string, *config.ChannelConfig) (tuner.Device, error) {
	return nil, errors.New("no tuner")
}

func TestServiceUpdaterDispatchesPerChannel(t *testing.T) {
	channels := config.ChannelsConfig{
		{Type: "GR", Channel: "27"},
		{Type: "GR", Channel: "26"},
	}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	mgr := newTestManager(t)
	sm := service.NewServiceManager(service.NewSQLiteStore(database), channels)
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	RegisterServiceUpdater(mgr, sm, stm, channels)
	if _, err := mgr.Enqueue(ServiceUpdaterKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		ServiceUpdaterKey:    true,
		"service-scan:GR:27": true,
		"service-scan:GR:26": true,
	})
}

func TestEPGGathererDispatchesPerNetwork(t *testing.T) {
	ctx := context.Background()
	channels := config.ChannelsConfig{
		{Type: "GR", Channel: "27"},
		{Type: "BS", Channel: "BS01"},
		{Type: "BS", Channel: "BS03"},
	}
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	serviceStore := service.NewSQLiteStore(database)
	sm := service.NewServiceManager(serviceStore, channels)
	if err := serviceStore.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{Id: "327360001", NetworkId: 32736, ServiceId: 1, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "BS01", []*service.Service{
		{Id: "0000400101", NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := serviceStore.ReplaceChannelServices(ctx, "BS", "BS03", []*service.Service{
		{Id: "0000400103", NetworkId: 4, ServiceId: 103, ChannelType: "BS", ChannelId: "BS03"},
	}); err != nil {
		t.Fatal(err)
	}

	programDatabase, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer programDatabase.Close()
	mgr := newTestManager(t)
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	RegisterEPGGatherer(mgr, program.NewProgramManager(program.NewSQLiteStore(programDatabase)), sm, stm, channels, 3)
	if _, err := mgr.Enqueue(EPGGathererKey); err != nil {
		t.Fatal(err)
	}
	waitForJobKeys(t, mgr, map[string]bool{
		EPGGathererKey:         true,
		"epg-gather:nid:32736": true,
		"epg-gather:nid:4":     true,
	})
}

func waitForJobKeys(t *testing.T, mgr *JobManager, expected map[string]bool) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		found := make(map[string]bool)
		for _, item := range mgr.GetJobs() {
			found[item.Key] = true
		}
		all := true
		for key := range expected {
			all = all && found[key]
		}
		if all {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("job keys not dispatched: %#v", mgr.GetJobs())
}
