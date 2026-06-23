package observability

import (
	"context"
	"testing"

	"github.com/21S1298001/Mahiron5/internal/config"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

func TestSetupTracesDisabledUsesNoopProvider(t *testing.T) {
	result := Setup(context.Background(), config.ObservabilityConfig{}, nil)
	if _, ok := result.TracerProvider.(tracenoop.TracerProvider); !ok {
		t.Fatalf("TracerProvider = %T, want noop.TracerProvider", result.TracerProvider)
	}
	if _, ok := result.MeterProvider.(noop.MeterProvider); !ok {
		t.Fatalf("MeterProvider = %T, want noop.MeterProvider", result.MeterProvider)
	}
	if err := result.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown() = %v", err)
	}
}

func TestRecordJobRunMetrics(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	initMetrics(provider)

	RecordJobRun(t.Context(), "test-job", "success", 123)
	RecordStreamSessionStart(t.Context(), "GR", "GR", "local", "success")
	RecordStreamSessionDuration(t.Context(), "GR", "GR", "local", 456)
	RecordTunerAcquire(t.Context(), "GR", "success", false, 12)

	var data metricdata.ResourceMetrics
	if err := reader.Collect(t.Context(), &data); err != nil {
		t.Fatal(err)
	}
	if got := int64Sum(data, MetricJobRuns); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricJobRuns, got)
	}
	if got := int64HistogramCount(data, MetricJobDuration); got != 1 {
		t.Fatalf("%s count = %d, want 1", MetricJobDuration, got)
	}
	if got := int64Sum(data, MetricStreamSessionStarts); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricStreamSessionStarts, got)
	}
	if got := int64HistogramCount(data, MetricStreamSessionDuration); got != 1 {
		t.Fatalf("%s count = %d, want 1", MetricStreamSessionDuration, got)
	}
	if got := int64Sum(data, MetricTunerAcquireRequests); got != 1 {
		t.Fatalf("%s = %d, want 1", MetricTunerAcquireRequests, got)
	}
	if got := int64HistogramCount(data, MetricTunerAcquireDuration); got != 1 {
		t.Fatalf("%s count = %d, want 1", MetricTunerAcquireDuration, got)
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

func int64Sum(data metricdata.ResourceMetrics, name string) int64 {
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name != name {
				continue
			}
			sum, ok := item.Data.(metricdata.Sum[int64])
			if !ok {
				return 0
			}
			var total int64
			for _, point := range sum.DataPoints {
				total += point.Value
			}
			return total
		}
	}
	return 0
}

func int64HistogramCount(data metricdata.ResourceMetrics, name string) uint64 {
	for _, scope := range data.ScopeMetrics {
		for _, item := range scope.Metrics {
			if item.Name != name {
				continue
			}
			histogram, ok := item.Data.(metricdata.Histogram[int64])
			if !ok {
				return 0
			}
			var total uint64
			for _, point := range histogram.DataPoints {
				total += point.Count
			}
			return total
		}
	}
	return 0
}
