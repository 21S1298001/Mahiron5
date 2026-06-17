package api

import (
	"context"
	"testing"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/program"
	"github.com/21S1298001/Mahiron5/service"
	apigen "github.com/21S1298001/Mahiron5/web/api/gen"
)

func testProgramHandler(t *testing.T) *Handler {
	t.Helper()
	pm := program.NewProgramManager(nil)
	if err := pm.Upsert(&program.Program{
		ID:        program.ProgramID(1, 101, 10),
		NetworkID: 1,
		ServiceID: 101,
		EventID:   10,
		StartAt:   2000,
		Duration:  30000,
		IsFree:    true,
		Name:      "second",
	}); err != nil {
		t.Fatal(err)
	}
	if err := pm.Upsert(&program.Program{
		ID:        program.ProgramID(1, 101, 9),
		NetworkID: 1,
		ServiceID: 101,
		EventID:   9,
		StartAt:   1000,
		Duration:  30000,
		IsFree:    true,
		Name:      "first",
	}); err != nil {
		t.Fatal(err)
	}

	return NewHandler(HandlerConfig{
		ProgramManager: pm,
		ServiceManager: service.NewServiceManager(&service.ServiceManagerConfig{
			Channels: config.ChannelsConfig{{Name: "NHK", Type: "GR", Channel: "27"}},
			Services: []*service.Service{{
				Id:        "0000100101",
				ServiceId: 101,
				NetworkId: 1,
				Name:      "NHK Service",
			}},
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
