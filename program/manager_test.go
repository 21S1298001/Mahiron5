package program

import (
	"context"
	"strings"
	"testing"

	"github.com/21S1298001/Mahiron5/db"
)

const sampleEIT = `{"originalNetworkId":32736,"serviceId":1024,"events":[{"eventId":12250,"startTime":1570917180000,"duration":420000,"scrambled":false,"descriptors":[{"$type":"ShortEvent","eventName":"気象情報・ニュース","text":"説明"},{"$type":"Component","streamContent":1,"componentType":179},{"$type":"AudioComponent","componentType":1,"componentTag":16,"mainComponent":true,"samplingRate":7,"lang":"jpn"},{"$type":"Content","nibbles":[[0,1,15,15]]}]}]}`

func newTestManager(t *testing.T) *ProgramManager {
	t.Helper()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	return NewProgramManager(NewSQLiteStore(database))
}

func TestReadEITJSONLDecodesDescriptors(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)

	if err := manager.ReadEITJSONL(ctx, strings.NewReader(sampleEIT+"\n")); err != nil {
		t.Fatal(err)
	}

	p, ok, err := manager.Get(ctx, ProgramID(32736, 1024, 12250))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("program not stored")
	}
	if p.Name != "気象情報・ニュース" {
		t.Fatalf("Name = %q", p.Name)
	}
	if p.Description != "説明" {
		t.Fatalf("Description = %q", p.Description)
	}
	if !p.IsFree {
		t.Fatal("IsFree = false")
	}
	if p.Video == nil || p.Video.StreamContent != 1 || p.Video.ComponentType != 179 {
		t.Fatalf("Video = %#v", p.Video)
	}
	if len(p.Audios) != 1 || p.Audios[0].SamplingRate == nil || *p.Audios[0].SamplingRate != 48000 {
		t.Fatalf("Audios = %#v", p.Audios)
	}
	if len(p.Genres) != 1 || p.Genres[0].Lv1 != 0 || p.Genres[0].Lv2 != 1 {
		t.Fatalf("Genres = %#v", p.Genres)
	}
}

func TestEITPFUpsertsExistingProgram(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	if err := manager.ReadEITJSONL(ctx, strings.NewReader(sampleEIT+"\n")); err != nil {
		t.Fatal(err)
	}

	update := strings.Replace(sampleEIT, "気象情報・ニュース", "延長後ニュース", 1)
	if err := manager.ReadEITJSONL(ctx, strings.NewReader(update+"\n")); err != nil {
		t.Fatal(err)
	}

	p, ok, err := manager.Get(ctx, ProgramID(32736, 1024, 12250))
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("program not stored")
	}
	if p.Name != "延長後ニュース" {
		t.Fatalf("Name = %q, want updated value", p.Name)
	}
}

func TestListFiltersAndSorts(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 2, 2), NetworkID: 1, ServiceID: 2, EventID: 2, StartAt: 2000},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 2, 1), NetworkID: 1, ServiceID: 2, EventID: 1, StartAt: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 3, 1), NetworkID: 1, ServiceID: 3, EventID: 1, StartAt: 500},
	}); err != nil {
		t.Fatal(err)
	}

	serviceID := uint16(2)
	programs, err := manager.List(ctx, Query{ServiceID: &serviceID})
	if err != nil {
		t.Fatal(err)
	}
	if len(programs) != 2 {
		t.Fatalf("len = %d, want 2", len(programs))
	}
	if programs[0].EventID != 1 || programs[1].EventID != 2 {
		t.Fatalf("programs not sorted by start time: %#v", programs)
	}
}

func TestListFiltersByID(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	wanted := ProgramID(1, 2, 1)
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: wanted, NetworkID: 1, ServiceID: 2, EventID: 1},
		{ID: ProgramID(1, 2, 2), NetworkID: 1, ServiceID: 2, EventID: 2},
	}); err != nil {
		t.Fatal(err)
	}
	programs, err := manager.List(ctx, Query{ID: &wanted})
	if err != nil {
		t.Fatal(err)
	}
	if len(programs) != 1 || programs[0].ID != wanted {
		t.Fatalf("programs = %#v, want ID %d", programs, wanted)
	}
}

func TestSQLiteStoreRejectsInvalidJSON(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	id := ProgramID(1, 2, 1)
	_, err = database.ExecContext(ctx, `INSERT INTO programs
		(id, event_id, service_id, network_id, start_at, duration, is_free, genres)
		VALUES (?, 1, 2, 1, 0, 0, 1, '{')`, id)
	if err != nil {
		t.Fatal(err)
	}
	store := NewSQLiteStore(database)
	if _, _, err := store.Get(ctx, id); err == nil {
		t.Fatal("Get succeeded with invalid genres JSON")
	}
	if _, err := store.List(ctx, Query{}); err == nil {
		t.Fatal("List succeeded with invalid genres JSON")
	}
}
