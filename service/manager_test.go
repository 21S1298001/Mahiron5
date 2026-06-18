package service

import (
	"context"
	"testing"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/db"
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
