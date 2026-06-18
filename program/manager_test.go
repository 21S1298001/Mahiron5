package program

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/21S1298001/Mahiron5/db"
)

func jsonUnmarshal(data []byte, v any) error { return json.Unmarshal(data, v) }

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

func TestReplaceServiceProgramsDeletesFutureAndKeepsPast(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	now := int64(10000)
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 2, 1), NetworkID: 1, ServiceID: 2, EventID: 1, StartAt: 1000, Duration: 1000},
		{ID: ProgramID(1, 2, 2), NetworkID: 1, ServiceID: 2, EventID: 2, StartAt: 5000, Duration: 1000},
		{ID: ProgramID(1, 2, 3), NetworkID: 1, ServiceID: 2, EventID: 3, StartAt: 9000, Duration: 2000},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReplaceServicePrograms(ctx, 1, 2, now, []*Program{
		{ID: ProgramID(1, 2, 4), NetworkID: 1, ServiceID: 2, EventID: 4, StartAt: 12000, Duration: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	serviceID := uint16(2)
	programs, err := manager.List(ctx, Query{ServiceID: &serviceID})
	if err != nil {
		t.Fatal(err)
	}
	if got, want := len(programs), 3; got != want {
		t.Fatalf("after replace programs = %d, want %d", got, want)
	}
	if programs[0].EventID != 1 || programs[1].EventID != 2 {
		t.Fatalf("past programs not preserved: %#v", programs)
	}
	if programs[2].EventID != 4 {
		t.Fatalf("newest kept = %d, want 4", programs[2].EventID)
	}
}

func TestReplaceServiceProgramsReplacesAcrossServices(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	if err := manager.store.UpsertAll(ctx, []*Program{
		{ID: ProgramID(1, 2, 1), NetworkID: 1, ServiceID: 2, EventID: 1, StartAt: 5000, Duration: 1000},
		{ID: ProgramID(1, 3, 1), NetworkID: 1, ServiceID: 3, EventID: 1, StartAt: 5000, Duration: 1000},
	}); err != nil {
		t.Fatal(err)
	}
	if err := manager.ReplaceServicePrograms(ctx, 1, 2, 0, nil); err != nil {
		t.Fatal(err)
	}
	other := uint16(3)
	got, err := manager.List(ctx, Query{ServiceID: &other})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("other service = %d, want 1", len(got))
	}
}

func TestSQLiteStoreRoundTripsExtendedAndRelatedAndSeries(t *testing.T) {
	ctx := context.Background()
	manager := newTestManager(t)
	id := ProgramID(1, 2, 1)
	nid, sid := uint16(1), uint16(2)
	program := &Program{
		ID:        id,
		NetworkID: nid,
		ServiceID: sid,
		EventID:   1,
		StartAt:   1000,
		Duration:  1000,
		Name:      "name",
		Extended:  map[string]string{"出演者": "foo", "概要": "bar"},
		RelatedItems: []RelatedItem{
			{Type: RelatedItemTypeShared, NetworkID: &nid, ServiceID: sid, EventID: 9},
		},
		Series: &Series{ID: 7, Repeat: 0, Pattern: 0, Episode: 1, LastEpisode: 12, Name: "series-name"},
	}
	if err := manager.store.UpsertAll(ctx, []*Program{program}); err != nil {
		t.Fatal(err)
	}
	got, ok, err := manager.Get(ctx, id)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("program not stored")
	}
	if got.Extended["出演者"] != "foo" {
		t.Fatalf("Extended[出演者] = %q", got.Extended["出演者"])
	}
	if len(got.RelatedItems) != 1 || got.RelatedItems[0].Type != RelatedItemTypeShared {
		t.Fatalf("RelatedItems = %#v", got.RelatedItems)
	}
	if got.Series == nil || got.Series.ID != 7 || got.Series.Name != "series-name" {
		t.Fatalf("Series = %#v", got.Series)
	}
}

func TestApplyDescriptorHandlesExtendedAndSeriesAndEventGroup(t *testing.T) {
	gt := 0x01
	prog := &Program{}
	applyDescriptor(prog, EITDescriptor{
		Type: "ExtendedEvent",
		Items: [][]string{
			{"出演者", "Foo"},
			{"概要", "Bar"},
		},
	})
	if prog.Extended["出演者"] != "Foo" || prog.Extended["概要"] != "Bar" {
		t.Fatalf("Extended = %#v", prog.Extended)
	}
	prog2 := &Program{}
	applyDescriptor(prog2, EITDescriptor{
		Type:       "Series",
		SeriesID:   ptrInt(11),
		SeriesName: "series-A",
	})
	if prog2.Series == nil || prog2.Series.ID != 11 || prog2.Series.Name != "series-A" {
		t.Fatalf("Series = %#v", prog2.Series)
	}
	prog3 := &Program{}
	applyDescriptor(prog3, EITDescriptor{
		Type:      "EventGroup",
		GroupType: &gt,
		Events:    []RelatedEvent{{ServiceID: 1, EventID: 2}},
	})
	if len(prog3.RelatedItems) != 1 || prog3.RelatedItems[0].Type != RelatedItemTypeShared {
		t.Fatalf("RelatedItems = %#v", prog3.RelatedItems)
	}
}

func TestEITDescriptorUnmarshalAcceptsLegacyAliases(t *testing.T) {
	cases := []string{
		`{"$type":"AudioComponent","languageCode":"jpn","mainComponentFlag":true}`,
		`{"$type":"AudioComponent","lang":"jpn","mainComponent":true}`,
	}
	for _, c := range cases {
		var d EITDescriptor
		if err := jsonUnmarshal([]byte(c), &d); err != nil {
			t.Fatalf("unmarshal: %v", err)
		}
		if d.Lang != "jpn" {
			t.Fatalf("Lang = %q, want jpn", d.Lang)
		}
		if d.MainComponent == nil || !*d.MainComponent {
			t.Fatalf("MainComponent = %#v", d.MainComponent)
		}
	}
}

func ptrInt(v int) *int { return &v }
