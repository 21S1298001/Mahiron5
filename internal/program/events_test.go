package program

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/db"
	"github.com/21S1298001/Mahiron5/internal/eventhub"
)

func TestProgramManagerPublishesCreateUpdateAndRemoveEvents(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	hub := eventhub.New()
	manager := NewProgramManager(NewSQLiteStore(database), hub)

	p := &Program{ID: ProgramID(1, 101, 1), NetworkID: 1, ServiceID: 101, EventID: 1, Name: "first"}
	if err := manager.UpsertPrograms(ctx, []*Program{p}); err != nil {
		t.Fatal(err)
	}
	manager.flushEvents()

	p.Name = "updated"
	if err := manager.UpsertPrograms(ctx, []*Program{p}); err != nil {
		t.Fatal(err)
	}
	manager.flushEvents()

	if err := manager.ReplaceServicePrograms(ctx, 1, 101, 0, nil); err != nil {
		t.Fatal(err)
	}
	manager.flushEvents()

	events := hub.Log()
	if got, want := len(events), 3; got != want {
		t.Fatalf("events length = %d, want %d: %#v", got, want, events)
	}
	if events[0].Type != eventhub.TypeCreate || events[1].Type != eventhub.TypeUpdate || events[2].Type != eventhub.TypeRemove {
		t.Fatalf("event types = %s/%s/%s", events[0].Type, events[1].Type, events[2].Type)
	}
	var removed map[string]int64
	if err := json.Unmarshal(events[2].Data, &removed); err != nil {
		t.Fatal(err)
	}
	if got, want := removed["id"], p.ID; got != want {
		t.Fatalf("remove payload id = %d, want %d", got, want)
	}
}
