package epg

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/db"
	"github.com/21S1298001/Mahiron5/internal/program"
)

const sampleEIT = `{"originalNetworkId":32736,"serviceId":1024,"events":[{"eventId":12250,"startTime":1570917180000,"duration":420000,"scrambled":false,"descriptors":[{"$type":"ShortEvent","eventName":"気象情報・ニュース","text":"説明"},{"$type":"Component","streamContent":1,"componentType":179},{"$type":"AudioComponent","componentType":1,"componentTag":16,"mainComponent":true,"samplingRate":7,"lang":"jpn"},{"$type":"Content","nibbles":[[0,1,15,15]]}]}]}`

func newTestProgramManager(t *testing.T) *program.ProgramManager {
	t.Helper()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	return program.NewProgramManager(program.NewSQLiteStore(database))
}

func TestReadEITJSONLDecodesDescriptors(t *testing.T) {
	ctx := context.Background()
	manager := newTestProgramManager(t)
	updater := NewUpdater(manager)

	if err := updater.ReadEITJSONL(ctx, strings.NewReader(sampleEIT+"\n")); err != nil {
		t.Fatal(err)
	}

	p, ok, err := manager.Get(ctx, program.ProgramID(32736, 1024, 12250))
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
	manager := newTestProgramManager(t)
	updater := NewUpdater(manager)
	if err := updater.ReadEITJSONL(ctx, strings.NewReader(sampleEIT+"\n")); err != nil {
		t.Fatal(err)
	}

	update := strings.Replace(sampleEIT, "気象情報・ニュース", "延長後ニュース", 1)
	if err := updater.ReadEITJSONL(ctx, strings.NewReader(update+"\n")); err != nil {
		t.Fatal(err)
	}

	p, ok, err := manager.Get(ctx, program.ProgramID(32736, 1024, 12250))
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

func TestApplyDescriptorHandlesExtendedAndSeriesAndEventGroup(t *testing.T) {
	gt := 0x01
	prog := &program.Program{}
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
	prog2 := &program.Program{}
	applyDescriptor(prog2, EITDescriptor{
		Type:       "Series",
		SeriesID:   ptrInt(11),
		SeriesName: "series-A",
	})
	if prog2.Series == nil || prog2.Series.ID != 11 || prog2.Series.Name != "series-A" {
		t.Fatalf("Series = %#v", prog2.Series)
	}
	prog3 := &program.Program{}
	applyDescriptor(prog3, EITDescriptor{
		Type:      "EventGroup",
		GroupType: &gt,
		Events:    []RelatedEvent{{ServiceID: 1, EventID: 2}},
	})
	if len(prog3.RelatedItems) != 1 || prog3.RelatedItems[0].Type != program.RelatedItemTypeShared {
		t.Fatalf("RelatedItems = %#v", prog3.RelatedItems)
	}
}

func TestEITDescriptorUnmarshalAcceptsLegacyAliases(t *testing.T) {
	// mirakc-arib serializes the ARIB 24-bit language code as a JSON number
	// (e.g. "jpn" -> 0x6A706E = 6975598 in decimal).
	cases := []struct {
		name      string
		body      string
		wantLang  string
		wantLang2 string
	}{
		{"legacy-string-aliases", `{"$type":"AudioComponent","languageCode":"jpn","mainComponentFlag":true}`, "jpn", ""},
		{"modern-fields", `{"$type":"AudioComponent","lang":"jpn","mainComponent":true}`, "jpn", ""},
		{"numeric-languageCode", `{"$type":"AudioComponent","languageCode":6975598,"mainComponent":true}`, "jpn", ""},
		{"numeric-languageCode2", `{"$type":"AudioComponent","lang":"jpn","languageCode2":6975598}`, "jpn", "jpn"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var d EITDescriptor
			if err := json.Unmarshal([]byte(c.body), &d); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if d.Lang != c.wantLang {
				t.Fatalf("Lang = %q, want %q", d.Lang, c.wantLang)
			}
			if d.Lang2 != c.wantLang2 {
				t.Fatalf("Lang2 = %q, want %q", d.Lang2, c.wantLang2)
			}
		})
	}
}

func ptrInt(v int) *int { return &v }
