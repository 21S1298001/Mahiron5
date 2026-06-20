package service

import (
	"context"
	"fmt"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/db"
)

func TestServiceManagerGetChannelsExcludesDisabledChannels(t *testing.T) {
	no := false
	yes := true
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	manager := NewServiceManager(NewSQLiteStore(database), config.ChannelsConfig{
		{Name: "NHK", Type: "GR", Channel: "27", IsDisabled: &no},
		{Name: "Disabled", Type: "GR", Channel: "28", IsDisabled: &yes},
	})

	channels := manager.GetChannels()
	if got, want := len(channels), 1; got != want {
		t.Fatalf("channels length = %d, want %d", got, want)
	}
	if got, want := channels[0].Channel, "27"; got != want {
		t.Fatalf("channel = %q, want %q", got, want)
	}
	if channel := manager.GetChannel("GR", "28"); channel != nil {
		t.Fatal("disabled channel should not be returned")
	}
}

func TestServiceManagerUpdateServicesAppendsAndUpdatesByID(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})

	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{
			Id:          "0000100101",
			ServiceId:   101,
			NetworkId:   1,
			Name:        "NHK",
			ChannelType: "GR",
			ChannelId:   "27",
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "BS", "101", []*Service{
		{
			Id:          "0000200102",
			ServiceId:   102,
			NetworkId:   2,
			Name:        "BS",
			ChannelType: "BS",
			ChannelId:   "101",
		},
	}); err != nil {
		t.Fatal(err)
	}

	services, err := manager.GetServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(services), 2; got != want {
		t.Fatalf("services length = %d, want %d", got, want)
	}

	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{
			Id:          "0000100101",
			ServiceId:   101,
			NetworkId:   1,
			Name:        "NHK Updated",
			ChannelType: "GR",
			ChannelId:   "27",
		},
	}); err != nil {
		t.Fatal(err)
	}

	services, err = manager.GetServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(services), 2; got != want {
		t.Fatalf("services length after update = %d, want %d", got, want)
	}
	svc, err := manager.GetServiceById(ctx, "100101")
	if err != nil {
		t.Fatal(err)
	}
	if svc == nil {
		t.Fatal("service not found")
	}
	if got, want := svc.Name, "NHK Updated"; got != want {
		t.Fatalf("updated service name = %q, want %q", got, want)
	}
}

func TestSQLiteStoreMovesServiceBetweenChannels(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	service := &Service{Id: "0000100101", ServiceId: 101, NetworkId: 1, Name: "NHK"}
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{service}); err != nil {
		t.Fatal(err)
	}
	if err := store.ReplaceChannelServices(ctx, "GR", "28", []*Service{service}); err != nil {
		t.Fatal(err)
	}
	old, err := store.GetByChannel(ctx, "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	moved, err := store.GetByChannel(ctx, "GR", "28")
	if err != nil {
		t.Fatal(err)
	}
	if len(old) != 0 || len(moved) != 1 {
		t.Fatalf("old=%d moved=%d, want old=0 moved=1", len(old), len(moved))
	}
}

func TestServiceManagerReconcileChannelsPrunesRemovedAndDisabled(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	for _, channel := range []ChannelKey{{Type: "GR", ID: "27"}, {Type: "GR", ID: "28"}, {Type: "BS", ID: "101"}} {
		service := &Service{Id: channel.Type + channel.ID, Name: channel.ID}
		if err := store.ReplaceChannelServices(ctx, channel.Type, channel.ID, []*Service{service}); err != nil {
			t.Fatal(err)
		}
	}
	disabled := true
	manager := NewServiceManager(store, config.ChannelsConfig{
		{Type: "GR", Channel: "27"},
		{Type: "GR", Channel: "28", IsDisabled: &disabled},
	})
	if err := manager.ReconcileChannels(ctx); err != nil {
		t.Fatal(err)
	}
	services, err := store.List(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(services) != 1 || services[0].ChannelId != "27" {
		t.Fatalf("services = %#v, want only GR/27", services)
	}
}

func TestServiceManagerEPGStatus(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, Name: "NHK", ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.SetEPGAttempt(ctx, 1, 101, 1000, "boom"); err != nil {
		t.Fatal(err)
	}
	services, err := manager.GetServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if services[0].EPG.LastError != "boom" {
		t.Fatalf("LastError = %q, want boom", services[0].EPG.LastError)
	}
	if services[0].EPG.LastAttemptAt == nil || *services[0].EPG.LastAttemptAt != 1000 {
		t.Fatalf("LastAttemptAt = %v, want 1000", services[0].EPG.LastAttemptAt)
	}
	if services[0].EPG.LastSuccessAt != nil {
		t.Fatalf("LastSuccessAt = %v, want nil", services[0].EPG.LastSuccessAt)
	}
	if err := manager.SetEPGSuccess(ctx, 1, 101, 2000); err != nil {
		t.Fatal(err)
	}
	services, err = manager.GetServices(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if services[0].EPG.LastError != "" {
		t.Fatalf("LastError = %q, want empty", services[0].EPG.LastError)
	}
	if services[0].EPG.LastSuccessAt == nil || *services[0].EPG.LastSuccessAt != 2000 {
		t.Fatalf("LastSuccessAt = %v, want 2000", services[0].EPG.LastSuccessAt)
	}
}

func TestServiceManagerEPGSummary(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	store := NewSQLiteStore(database)
	manager := NewServiceManager(store, config.ChannelsConfig{})
	if err := store.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100102", ServiceId: 102, NetworkId: 1, ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100103", ServiceId: 103, NetworkId: 1, ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.SetEPGSuccess(ctx, 1, 101, 1000); err != nil {
		t.Fatal(err)
	}
	if err := manager.SetEPGAttempt(ctx, 1, 102, 2000, "boom"); err != nil {
		t.Fatal(err)
	}
	if err := manager.SetEPGAttempt(ctx, 1, 103, 3000, ""); err != nil {
		t.Fatal(err)
	}
	stale, failed, lastSuccess, err := manager.EPGSummary(ctx, 500, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if stale != 3 {
		t.Errorf("stale = %d, want 3 (everything older than 500ms)", stale)
	}
	if failed != 1 {
		t.Errorf("failed = %d, want 1", failed)
	}
	if lastSuccess == nil || *lastSuccess != 1000 {
		t.Errorf("lastSuccess = %v, want 1000", lastSuccess)
	}
	stale, _, _, err = manager.EPGSummary(ctx, 5000, 4000)
	if err != nil {
		t.Fatal(err)
	}
	if stale != 2 {
		t.Errorf("stale = %d, want 2 with larger window", stale)
	}
}

func TestNewNetworkIDsFromDiff(t *testing.T) {
	svc := func(nid, sid uint16) *Service {
		return &Service{Id: idFor(nid, sid), NetworkId: nid, ServiceId: sid}
	}
	before := map[string]struct{}{
		idFor(1, 101): {}, // already known NID 1, SID 101
		idFor(2, 201): {}, // already known NID 2, SID 201
	}
	scanned := []*Service{
		svc(1, 101), // already known
		svc(1, 102), // new service on existing NID 1
		svc(2, 201), // already known
		svc(3, 301), // brand new NID
		svc(3, 302), // same new NID 3 (dedupe)
	}
	got := newNetworkIDsFromDiff(before, scanned)
	want := map[uint16]bool{1: true, 3: true}
	if len(got) != len(want) {
		t.Fatalf("newNetworkIDsFromDiff = %v, want NIDs %v", got, want)
	}
	for _, nid := range got {
		if !want[nid] {
			t.Errorf("unexpected NID %d in result %v", nid, got)
		}
	}
}

func TestNewNetworkIDsFromDiffEmptyInputs(t *testing.T) {
	if got := newNetworkIDsFromDiff(nil, nil); got != nil {
		t.Errorf("nil scanned = %v, want nil", got)
	}
	before := map[string]struct{}{idFor(1, 101): {}}
	allKnown := []*Service{
		{Id: idFor(1, 101), NetworkId: 1, ServiceId: 101},
	}
	if got := newNetworkIDsFromDiff(before, allKnown); got != nil {
		t.Errorf("all-known scanned = %v, want nil", got)
	}
}

func idFor(nid, sid uint16) string {
	return fmt.Sprintf("%05d%05d", nid, sid)
}
