package apigen

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"go.opentelemetry.io/otel/trace/noop"
)

type flushRecorder struct {
	header  http.Header
	body    bytes.Buffer
	flushes chan string
}

func newFlushRecorder() *flushRecorder {
	return &flushRecorder{
		header:  make(http.Header),
		flushes: make(chan string, 1),
	}
}

func (r *flushRecorder) Header() http.Header {
	return r.header
}

func (r *flushRecorder) WriteHeader(statusCode int) {}

func (r *flushRecorder) Write(p []byte) (int, error) {
	return r.body.Write(p)
}

func (r *flushRecorder) Flush() {
	select {
	case r.flushes <- r.body.String():
	default:
	}
}

func TestEncodeGetEventsStreamResponseFlushesInitialBytes(t *testing.T) {
	reader, writer := io.Pipe()
	defer reader.Close()
	defer writer.Close()

	recorder := newFlushRecorder()
	errCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, span := noop.NewTracerProvider().Tracer("test").Start(ctx, "test")
	defer span.End()

	go func() {
		errCh <- encodeGetEventsStreamResponse(&GetEventsStreamOK{Data: reader}, recorder, span)
	}()

	if _, err := writer.Write([]byte("[\n")); err != nil {
		t.Fatal(err)
	}

	select {
	case flushed := <-recorder.flushes:
		if flushed != "[\n" {
			t.Fatalf("flushed body = %q, want open JSON array", flushed)
		}
	case <-time.After(time.Second):
		t.Fatal("stream response did not flush initial bytes")
	}

	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("stream response encoder did not finish")
	}
}
