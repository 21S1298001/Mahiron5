package api

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/db"
	"github.com/21S1298001/Mahiron5/internal/job"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/service"
	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
)

func newStatusHandler(t *testing.T) (*Handler, *job.JobManager, *service.ServiceManager, *program.ProgramManager, *sql.DB) {
	t.Helper()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	mgr, err := job.NewManager(job.Config{MaxHistory: 10})
	if err != nil {
		t.Fatal(err)
	}
	store := service.NewSQLiteStore(database)
	sm := service.NewServiceManager(store, config.ChannelsConfig{})
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	return NewHandler(HandlerConfig{ServiceManager: sm, ProgramManager: pm, JobManager: mgr, EpgStaleAfter: 5000}), mgr, sm, pm, database
}

func TestGetStatusExposesEPGSnapshot(t *testing.T) {
	ctx := context.Background()
	handler, mgr, sm, pm, database := newStatusHandler(t)
	store := service.NewSQLiteStore(database)
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100102", ServiceId: 102, NetworkId: 1, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := sm.SetEPGSuccess(ctx, 1, 101, 1000); err != nil {
		t.Fatal(err)
	}
	if err := sm.SetEPGAttempt(ctx, 1, 102, 2000, "boom"); err != nil {
		t.Fatal(err)
	}
	if err := pm.ReplaceServicePrograms(ctx, 1, 101, 0, []*program.Program{
		{ID: program.ProgramID(1, 101, 9), NetworkID: 1, ServiceID: 101, EventID: 9, StartAt: 1000, Duration: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	epgBlock := make(chan struct{})
	t.Cleanup(func() { close(epgBlock) })
	if _, err := mgr.EnqueueDefinition(job.JobDefinition{Key: "epg-gather:nid:1", Name: "EPG Gather NID 1", Handler: func(ctx context.Context) error {
		<-epgBlock
		return nil
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.EnqueueDefinition(job.JobDefinition{Key: "service-scan:GR:27", Name: "Service Scan", Handler: func(context.Context) error { return nil }}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	res, err := handler.GetStatus(ctx)
	if err != nil {
		t.Fatal(err)
	}
	status, ok := res.(*apigen.Status)
	if !ok {
		t.Fatalf("response type = %T, want *Status", res)
	}
	if !status.Epg.IsSet() {
		t.Fatal("status.Epg is unset")
	}
	epg := status.Epg.Value
	if got, want := len(epg.GatheringNetworks), 1; got != want {
		t.Fatalf("GatheringNetworks = %d, want %d", got, want)
	}
	if epg.GatheringNetworks[0] != 1 {
		t.Errorf("GatheringNetworks[0] = %d, want 1", epg.GatheringNetworks[0])
	}
	if epg.StoredEvents.Value != 1 {
		t.Errorf("StoredEvents = %d, want 1", epg.StoredEvents.Value)
	}
	if epg.StaleServices.Value != 2 {
		t.Errorf("StaleServices = %d, want 2 (both stale with 5000ms window vs attempt=2000)", epg.StaleServices.Value)
	}
	if epg.FailedServices.Value != 1 {
		t.Errorf("FailedServices = %d, want 1", epg.FailedServices.Value)
	}
	if epg.LastUpdatedAt.Value != 1000 {
		t.Errorf("LastUpdatedAt = %d, want 1000", epg.LastUpdatedAt.Value)
	}
}
