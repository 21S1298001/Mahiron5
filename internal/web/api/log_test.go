package api

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/observability"
	apigen "github.com/21S1298001/Mahiron5/internal/web/api/gen"
)

func TestGetLogReturnsSnapshot(t *testing.T) {
	store := observability.NewLogStore(10)
	if _, err := store.Write([]byte("stored\n")); err != nil {
		t.Fatal(err)
	}
	handler := NewHandler(HandlerConfig{LogStore: store})

	res, err := handler.GetLog(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	ok, okType := res.(*apigen.GetLogOK)
	if !okType {
		t.Fatalf("response = %T, want *GetLogOK", res)
	}
	data, err := io.ReadAll(ok.Data)
	if err != nil {
		t.Fatal(err)
	}
	if string(data) != "stored\n" {
		t.Fatalf("log data = %q, want stored log", string(data))
	}
}

func TestGetLogStreamReturnsNewLogs(t *testing.T) {
	store := observability.NewLogStore(10)
	handler := NewHandler(HandlerConfig{LogStore: store})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	res, err := handler.GetLogStream(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ok, okType := res.(*apigen.GetLogStreamOK)
	if !okType {
		t.Fatalf("response = %T, want *GetLogStreamOK", res)
	}
	if closer, ok := ok.Data.(io.Closer); ok {
		defer closer.Close()
	}

	if _, err := store.Write([]byte("streamed\n")); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len("streamed\n"))
	if _, err := io.ReadFull(ok.Data, buf); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(buf), "streamed") {
		t.Fatalf("stream data = %q, want streamed log", string(buf))
	}
}
