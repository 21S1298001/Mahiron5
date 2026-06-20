package service

import (
	"context"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/db"
	"github.com/21S1298001/Mahiron5/internal/eventhub"
)

func TestServiceManagerPublishesCreateUpdateRemoveAndEPGUpdateEvents(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	hub := eventhub.New()
	manager := NewServiceManager(NewSQLiteStore(database), config.ChannelsConfig{
		{Type: "GR", Channel: "27", Name: "NHK"},
	}, hub)

	if err := manager.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "0000100101", NetworkId: 1, ServiceId: 101, Name: "first", ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "0000100101", NetworkId: 1, ServiceId: 101, Name: "updated", ChannelType: "GR", ChannelId: "27"},
		{Id: "0000100102", NetworkId: 1, ServiceId: 102, Name: "second", ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReplaceChannelServices(ctx, "GR", "27", []*Service{
		{Id: "0000100102", NetworkId: 1, ServiceId: 102, Name: "second", ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.SetEPGSuccess(ctx, 1, 102, 1000); err != nil {
		t.Fatal(err)
	}

	types := make([]string, 0, len(hub.Log()))
	for _, event := range hub.Log() {
		types = append(types, event.Type)
	}
	want := []string{eventhub.TypeCreate, eventhub.TypeUpdate, eventhub.TypeCreate, eventhub.TypeRemove, eventhub.TypeUpdate}
	if len(types) != len(want) {
		t.Fatalf("event types = %#v, want %#v", types, want)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("event types = %#v, want %#v", types, want)
		}
	}
}
