package job

import (
	"errors"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/config"
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
	mgr := newTestManager(t)
	sm := service.NewServiceManager(&service.ServiceManagerConfig{Channels: channels})
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
	channels := config.ChannelsConfig{
		{Type: "GR", Channel: "27"},
		{Type: "BS", Channel: "BS01"},
		{Type: "BS", Channel: "BS03"},
	}
	services := []*service.Service{
		{NetworkId: 32736, ServiceId: 1, ChannelType: "GR", ChannelId: "27"},
		{NetworkId: 4, ServiceId: 101, ChannelType: "BS", ChannelId: "BS01"},
		{NetworkId: 4, ServiceId: 103, ChannelType: "BS", ChannelId: "BS03"},
	}
	mgr := newTestManager(t)
	sm := service.NewServiceManager(&service.ServiceManagerConfig{Channels: channels, Services: services})
	stm := stream.NewStreamManager(stream.StreamManagerConfig{Channels: channels, TunerManager: noTunerManager{}})
	RegisterEPGGatherer(mgr, program.NewProgramManager(nil), sm, stm, channels)
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
