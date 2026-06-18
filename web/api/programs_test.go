package api

import (
	"context"
	"testing"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/db"
	"github.com/21S1298001/Mahiron5/program"
	"github.com/21S1298001/Mahiron5/service"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
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
