package observability

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestLogStoreSnapshotKeepsNewestRecords(t *testing.T) {
	store := NewLogStore(2)
	if _, err := store.Write([]byte("first\nsecond\nthird\n")); err != nil {
		t.Fatal(err)
	}

	got := string(store.Snapshot())
	if got != "second\nthird\n" {
		t.Fatalf("Snapshot() = %q, want newest records", got)
	}
}

func TestLogStoreSubscribeReceivesNewRecords(t *testing.T) {
	store := NewLogStore(10)
	reader, unsubscribe := store.Subscribe()
	defer reader.Close()
	defer unsubscribe()

	if _, err := store.Write([]byte("live\n")); err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, len("live\n"))
	if _, err := io.ReadFull(reader, buf); err != nil {
		t.Fatal(err)
	}
	if string(buf) != "live\n" {
		t.Fatalf("read = %q, want live log", string(buf))
	}
}

func TestLogStoreSubscribeCloseUnblocksReader(t *testing.T) {
	store := NewLogStore(10)
	reader, unsubscribe := store.Subscribe()
	done := make(chan error, 1)
	go func() {
		_, err := reader.Read(make([]byte, 1))
		done <- err
	}()

	unsubscribe()
	reader.Close()

	select {
	case err := <-done:
		if err != io.EOF {
			t.Fatalf("Read() error = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Fatal("reader did not unblock")
	}
}

func TestLogStoreRecordsDroppedSubscriberChunks(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	initMetrics(provider)

	store := NewLogStore(200)
	stream, unsubscribe := store.Subscribe()
	defer stream.Close()
	defer unsubscribe()

	for range 129 {
		if _, err := store.Write([]byte("line\n")); err != nil {
			t.Fatal(err)
		}
	}

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	if got := int64Sum(data, MetricLogsDropped); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricLogsDropped, got)
	}
}

func TestFanoutHandlerUsesEnabledHandlers(t *testing.T) {
	info := &recordingHandler{level: slog.LevelInfo}
	warn := &recordingHandler{level: slog.LevelWarn}
	logger := slog.New(newFanoutHandler(info, warn))

	logger.Info("hello")
	logger.Warn("careful")

	if strings.Join(info.messages, ",") != "hello,careful" {
		t.Fatalf("info handler messages = %v", info.messages)
	}
	if strings.Join(warn.messages, ",") != "careful" {
		t.Fatalf("warn handler messages = %v", warn.messages)
	}
}

type recordingHandler struct {
	level    slog.Level
	messages []string
}

func (h *recordingHandler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *recordingHandler) Handle(_ context.Context, record slog.Record) error {
	h.messages = append(h.messages, record.Message)
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler {
	return h
}

func (h *recordingHandler) WithGroup(string) slog.Handler {
	return h
}
