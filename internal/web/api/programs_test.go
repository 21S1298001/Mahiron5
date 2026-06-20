package api

import (
	"context"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/db"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/service"
	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
)

func testProgramHandler(t *testing.T) *Handler {
	t.Helper()
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	if err := pm.UpsertEITSection(ctx, &program.EITSection{
		OriginalNetworkID: 1,
		ServiceID:         101,
		Events: []program.EITEvent{
			{EventID: 10, StartTime: 2000, Duration: 30000, Scrambled: false,
				Descriptors: []program.EITDescriptor{
					{Type: "ShortEvent", EventName: "second"},
				},
			},
			{EventID: 9, StartTime: 1000, Duration: 30000, Scrambled: false,
				Descriptors: []program.EITDescriptor{
					{Type: "ShortEvent", EventName: "first"},
				},
			},
		},
	}); err != nil {
		t.Fatal(err)
	}

	serviceStore := service.NewSQLiteStore(database)
	if err := serviceStore.ReplaceChannelServices(ctx, "GR", "27", []*service.Service{
		{Id: "0000100101", ServiceId: 101, NetworkId: 1, Name: "NHK Service", ChannelType: "GR", ChannelId: "27"},
	}); err != nil {
		t.Fatal(err)
	}
	return NewHandler(HandlerConfig{
		ProgramManager: pm,
		ServiceManager: service.NewServiceManager(serviceStore, config.ChannelsConfig{
			{Name: "NHK", Type: "GR", Channel: "27"},
		}),
	})
}

func TestGetProgramsFiltersAndSorts(t *testing.T) {
	handler := testProgramHandler(t)

	res, err := handler.GetPrograms(context.Background(), apigen.GetProgramsParams{
		ServiceId: apigen.NewOptInt(101),
	})
	if err != nil {
		t.Fatal(err)
	}
	programs, ok := res.(*apigen.GetProgramsOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetProgramsOKApplicationJSON", res)
	}
	if got, want := len(*programs), 2; got != want {
		t.Fatalf("programs length = %d, want %d", got, want)
	}
	if got, want := (*programs)[0].Name.Value, "first"; got != want {
		t.Fatalf("first program name = %q, want %q", got, want)
	}
}

func TestGetProgramReturnsProgramAndNotFound(t *testing.T) {
	handler := testProgramHandler(t)

	res, err := handler.GetProgram(context.Background(), apigen.GetProgramParams{ID: program.ProgramID(1, 101, 9)})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.Program); !ok {
		t.Fatalf("response type = %T, want *Program", res)
	}

	res, err = handler.GetProgram(context.Background(), apigen.GetProgramParams{ID: 999})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.(*apigen.ErrorStatusCode); !ok {
		t.Fatalf("response type = %T, want *ErrorStatusCode", res)
	}
}

func TestGetServicePrograms(t *testing.T) {
	handler := testProgramHandler(t)

	res, err := handler.GetServicePrograms(context.Background(), apigen.GetServiceProgramsParams{ID: 100101})
	if err != nil {
		t.Fatal(err)
	}
	programs, ok := res.(*apigen.GetServiceProgramsOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetServiceProgramsOKApplicationJSON", res)
	}
	if got, want := len(*programs), 2; got != want {
		t.Fatalf("programs length = %d, want %d", got, want)
	}
}

func TestApiProgramExposesExtendedRelatedAndSeries(t *testing.T) {
	ctx := context.Background()
	database, err := db.OpenInMemory()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { database.Close() })
	pm := program.NewProgramManager(program.NewSQLiteStore(database))
	id := program.ProgramID(1, 101, 7)
	nid := uint16(1)
	if err := pm.ReplaceServicePrograms(ctx, 1, 101, 0, []*program.Program{
		{
			ID:        id,
			NetworkID: 1,
			ServiceID: 101,
			EventID:   7,
			StartAt:   1000,
			Duration:  1000,
			Extended:  map[string]string{"出演者": "Foo"},
			RelatedItems: []program.RelatedItem{
				{Type: program.RelatedItemTypeShared, NetworkID: &nid, ServiceID: 101, EventID: 9},
			},
			Series: &program.Series{ID: 5, Pattern: 1, Episode: 1, LastEpisode: 12, Name: "series"},
		},
	}); err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(HandlerConfig{ProgramManager: pm})
	res, err := handler.GetProgram(context.Background(), apigen.GetProgramParams{ID: id})
	if err != nil {
		t.Fatal(err)
	}
	p, ok := res.(*apigen.Program)
	if !ok {
		t.Fatalf("response type = %T, want *Program", res)
	}
	if !p.Extended.IsSet() {
		t.Fatal("Extended not set")
	}
	if p.Extended.Value["出演者"] != "Foo" {
		t.Errorf("Extended[出演者] = %q, want Foo", p.Extended.Value["出演者"])
	}
	if len(p.RelatedItems) != 1 {
		t.Fatalf("RelatedItems = %d, want 1", len(p.RelatedItems))
	}
	if p.RelatedItems[0].Type.Value != apigen.RelatedItemTypeShared {
		t.Errorf("RelatedItem.Type = %v, want shared", p.RelatedItems[0].Type.Value)
	}
	if !p.Series.IsSet() {
		t.Fatal("Series not set")
	}
	if p.Series.Value.ID.Value != 5 {
		t.Errorf("Series.ID = %d, want 5", p.Series.Value.ID.Value)
	}
}

func TestApiProgramRelatedItemsEmptyWhenNone(t *testing.T) {
	handler := testProgramHandler(t)
	res, err := handler.GetProgram(context.Background(), apigen.GetProgramParams{ID: program.ProgramID(1, 101, 9)})
	if err != nil {
		t.Fatal(err)
	}
	p := res.(*apigen.Program)
	if p.RelatedItems == nil {
		t.Fatal("RelatedItems should be a non-nil empty slice")
	}
	if len(p.RelatedItems) != 0 {
		t.Errorf("RelatedItems = %d, want 0", len(p.RelatedItems))
	}
	if p.Extended.IsSet() {
		t.Errorf("Extended = %#v, want unset", p.Extended)
	}
	if p.Series.IsSet() {
		t.Errorf("Series = %#v, want unset", p.Series)
	}
}
