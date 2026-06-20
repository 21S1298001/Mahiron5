package api

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"testing"

	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
)

func TestGetEventsReturnsCurrentStateEvents(t *testing.T) {
	handler := testProgramHandler(t)

	res, err := handler.GetEvents(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	events, ok := res.(*apigen.GetEventsOKApplicationJSON)
	if !ok {
		t.Fatalf("response type = %T, want *GetEventsOKApplicationJSON", res)
	}
	if got, want := len(*events), 3; got != want {
		t.Fatalf("events length = %d, want %d", got, want)
	}

	first := (*events)[0]
	if first.Resource != apigen.EventResourceProgram {
		t.Fatalf("first resource = %q, want program", first.Resource)
	}
	if first.Type != apigen.EventTypeUpdate {
		t.Fatalf("first type = %q, want update", first.Type)
	}
	var name string
	if err := json.Unmarshal(first.Data["name"], &name); err != nil {
		t.Fatal(err)
	}
	if got, want := name, "first"; got != want {
		t.Fatalf("first event data name = %q, want %q", got, want)
	}
}

func TestGetEventsStreamWritesOpenJSONArraySnapshotEvents(t *testing.T) {
	handler := testProgramHandler(t)

	var buf bytes.Buffer
	if err := writeEventsOpenJSONArraySnapshot(context.Background(), &buf, handler, apigen.GetEventsStreamParams{
		Resource: apigen.NewOptGetEventsStreamResource(apigen.GetEventsStreamResourceProgram),
		Type:     apigen.NewOptGetEventsStreamType(apigen.GetEventsStreamTypeUpdate),
	}); err != nil {
		t.Fatal(err)
	}

	body := buf.String()
	if !strings.HasPrefix(body, "[\n") {
		t.Fatalf("body prefix = %q, want open JSON array", body)
	}
	if got, want := strings.Count(body, "\n,\n"), 2; got != want {
		t.Fatalf("event separator count = %d, want %d\n%s", got, want, body)
	}
	lines := strings.Split(body, "\n")
	var first apigen.Event
	if err := json.Unmarshal([]byte(lines[1]), &first); err != nil {
		t.Fatal(err)
	}
	if first.Resource != apigen.EventResourceProgram {
		t.Fatalf("first resource = %q, want program", first.Resource)
	}
	var name string
	if err := json.Unmarshal(first.Data["name"], &name); err != nil {
		t.Fatal(err)
	}
	if got, want := name, "first"; got != want {
		t.Fatalf("first event data name = %q, want %q", got, want)
	}
}

func TestGetEventsStreamFiltersOpenJSONArraySnapshotEvents(t *testing.T) {
	handler := testProgramHandler(t)

	var buf bytes.Buffer
	if err := writeEventsOpenJSONArraySnapshot(context.Background(), &buf, handler, apigen.GetEventsStreamParams{
		Type: apigen.NewOptGetEventsStreamType(apigen.GetEventsStreamTypeRemove),
	}); err != nil {
		t.Fatal(err)
	}
	if got, want := buf.String(), "[\n"; got != want {
		t.Fatalf("remove events body = %q, want %q", got, want)
	}
}
