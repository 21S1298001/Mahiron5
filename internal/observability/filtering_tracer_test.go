package observability

import (
	"context"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestSetupTracesDisabledUsesNoopProvider(t *testing.T) {
	result := Setup(context.Background(), config.ObservabilityConfig{}, nil)
	if _, ok := result.TracerProvider.(noop.TracerProvider); !ok {
		t.Fatalf("TracerProvider = %T, want noop.TracerProvider", result.TracerProvider)
	}
	if err := result.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
}

func TestFilteringTracerProviderSkipsExcludedSpans(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	filtered := NewFilteringTracerProvider(provider, []string{"GetChannelStream"})
	tracer := filtered.Tracer("test")

	_, statusSpan := tracer.Start(context.Background(), "GetStatus")
	statusSpan.End()
	_, streamSpan := tracer.Start(context.Background(), "GetChannelStream")
	streamSpan.End()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans = %d, want 1", len(spans))
	}
	if spans[0].Name() != "GetStatus" {
		t.Fatalf("span name = %q, want GetStatus", spans[0].Name())
	}
}
