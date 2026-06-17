package program

import (
	"context"
	"strings"
	"testing"
)

const sampleEIT = `{"originalNetworkId":32736,"serviceId":1024,"events":[{"eventId":12250,"startTime":1570917180000,"duration":420000,"scrambled":false,"descriptors":[{"$type":"ShortEvent","eventName":"気象情報・ニュース","text":"説明"},{"$type":"Component","streamContent":1,"componentType":179},{"$type":"AudioComponent","componentType":1,"componentTag":16,"mainComponent":true,"samplingRate":7,"lang":"jpn"},{"$type":"Content","nibbles":[[0,1,15,15]]}]}]}`

func TestReadEITJSONLDecodesDescriptors(t *testing.T) {
	manager := NewProgramManager(nil)

	if err := manager.ReadEITJSONL(context.Background(), strings.NewReader(sampleEIT+"\n")); err != nil {
		t.Fatal(err)
	}

	p, ok := manager.Get(ProgramID(32736, 1024, 12250))
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
	manager := NewProgramManager(nil)
	if err := manager.ReadEITJSONL(context.Background(), strings.NewReader(sampleEIT+"\n")); err != nil {
		t.Fatal(err)
	}

	update := strings.Replace(sampleEIT, "気象情報・ニュース", "延長後ニュース", 1)
	if err := manager.ReadEITJSONL(context.Background(), strings.NewReader(update+"\n")); err != nil {
		t.Fatal(err)
	}

	p, ok := manager.Get(ProgramID(32736, 1024, 12250))
	if !ok {
		t.Fatal("program not stored")
	}
	if p.Name != "延長後ニュース" {
		t.Fatalf("Name = %q, want updated value", p.Name)
	}
}

func TestListFiltersAndSorts(t *testing.T) {
	manager := NewProgramManager(nil)
	if err := manager.Upsert(&Program{ID: ProgramID(1, 2, 2), NetworkID: 1, ServiceID: 2, EventID: 2, StartAt: 2000}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Upsert(&Program{ID: ProgramID(1, 2, 1), NetworkID: 1, ServiceID: 2, EventID: 1, StartAt: 1000}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Upsert(&Program{ID: ProgramID(1, 3, 1), NetworkID: 1, ServiceID: 3, EventID: 1, StartAt: 500}); err != nil {
		t.Fatal(err)
	}

	serviceID := uint16(2)
	programs := manager.List(Query{ServiceID: &serviceID})
	if len(programs) != 2 {
		t.Fatalf("len = %d, want 2", len(programs))
	}
	if programs[0].EventID != 1 || programs[1].EventID != 2 {
		t.Fatalf("programs not sorted by start time: %#v", programs)
	}
}
