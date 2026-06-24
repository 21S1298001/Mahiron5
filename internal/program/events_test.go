package program

import (
	"context"
	"testing"

	"github.com/21S1298001/mahiron/internal/db"
)

type publishedProgramEvent struct {
	typ      string
	data     map[string]any
	removeID int64
}

type fakeProgramEventPublisher struct {
	events []publishedProgramEvent
}

func (p *fakeProgramEventPublisher) PublishProgramEvent(typ string, data map[string]any) {
	p.events = append(p.events, publishedProgramEvent{typ: typ, data: data})
}

func TestProgramManagerPublishesCreateUpdateAndRemoveEvents(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	publisher := &fakeProgramEventPublisher{}
	manager := NewProgramManager(NewSQLiteStore(database), publisher)

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

	events := publisher.events
	if got, want := len(events), 3; got != want {
		t.Fatalf("events length = %d, want %d: %#v", got, want, events)
	}
	if events[0].typ != eventTypeCreate || events[1].typ != eventTypeUpdate || events[2].typ != eventTypeRemove {
		t.Fatalf("event types = %s/%s/%s", events[0].typ, events[1].typ, events[2].typ)
	}
	if got, want := events[2].data["id"], p.ID; got != want {
		t.Fatalf("remove payload id = %v, want %d", got, want)
	}
}
