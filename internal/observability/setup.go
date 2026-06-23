package observability

import (
	"context"
	"errors"
	"log/slog"
	"os"

	"github.com/21S1298001/Mahiron5/internal/config"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otelmetric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

const instrumentationName = "github.com/21S1298001/Mahiron5"

type SetupResult struct {
	LogStore       *LogStore
	MeterProvider  otelmetric.MeterProvider
	TracerProvider trace.TracerProvider
	Shutdown       func(context.Context) error
}

func Setup(ctx context.Context, cfg config.ObservabilityConfig, level slog.Leveler) SetupResult {
	store := NewLogStore(defaultLogCapacity)
	stderr := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	apiLogs := slog.NewTextHandler(store, &slog.HandlerOptions{Level: level})

	var shutdowns []func(context.Context) error
	handlers := []slog.Handler{stderr, apiLogs}

	if cfg.Endpoint != "" && cfg.Logs.Enabled {
		handler, cleanup, err := newOTelLogHandler(ctx, cfg)
		if err != nil {
			slog.New(stderr).Warn("failed to initialize OTLP log exporter", "err", err)
		} else {
			handlers = append(handlers, handler)
			shutdowns = append(shutdowns, cleanup)
		}
	}

	slog.SetDefault(slog.New(newFanoutHandler(handlers...)))

	tracerProvider := trace.TracerProvider(tracenoop.NewTracerProvider())
	if cfg.Endpoint != "" && cfg.Traces.Enabled {
		provider, cleanup, err := newOTelTracerProvider(ctx, cfg)
		if err != nil {
			slog.Warn("failed to initialize OTLP trace exporter", "err", err)
		} else {
			tracerProvider = provider
			shutdowns = append(shutdowns, cleanup)
		}
	}
	otel.SetTracerProvider(tracerProvider)

	meterProvider := otelmetric.MeterProvider(noop.NewMeterProvider())
	if cfg.Endpoint != "" && cfg.Metrics.Enabled {
		provider, cleanup, err := newOTelMeterProvider(ctx, cfg)
		if err != nil {
			slog.Warn("failed to initialize OTLP metric exporter", "err", err)
		} else {
			meterProvider = provider
			shutdowns = append(shutdowns, cleanup)
		}
	}
	otel.SetMeterProvider(meterProvider)
	initMetrics(meterProvider)

	return SetupResult{
		LogStore:       store,
		MeterProvider:  meterProvider,
		TracerProvider: tracerProvider,
		Shutdown:       shutdownAll(shutdowns...),
	}
}

func newOTelLogHandler(ctx context.Context, cfg config.ObservabilityConfig) (slog.Handler, func(context.Context) error, error) {
	options := []otlploggrpc.Option{otlploggrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		options = append(options, otlploggrpc.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		options = append(options, otlploggrpc.WithHeaders(cfg.Headers))
	}

	exporter, err := otlploggrpc.New(ctx, options...)
	if err != nil {
		return nil, nil, err
	}

	res, err := newResource(cfg)
	if err != nil {
		return nil, nil, err
	}

	provider := log.NewLoggerProvider(
		log.WithResource(res),
		log.WithProcessor(log.NewBatchProcessor(exporter)),
	)
	handler := otelslog.NewHandler(instrumentationName, otelslog.WithLoggerProvider(provider))
	return handler, provider.Shutdown, nil
}

func newOTelTracerProvider(ctx context.Context, cfg config.ObservabilityConfig) (trace.TracerProvider, func(context.Context) error, error) {
	options := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		options = append(options, otlptracegrpc.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		options = append(options, otlptracegrpc.WithHeaders(cfg.Headers))
	}

	exporter, err := otlptracegrpc.New(ctx, options...)
	if err != nil {
		return nil, nil, err
	}
	res, err := newResource(cfg)
	if err != nil {
		return nil, nil, err
	}
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(exporter),
	)
	return provider, provider.Shutdown, nil
}

func newOTelMeterProvider(ctx context.Context, cfg config.ObservabilityConfig) (otelmetric.MeterProvider, func(context.Context) error, error) {
	options := []otlpmetricgrpc.Option{otlpmetricgrpc.WithEndpoint(cfg.Endpoint)}
	if cfg.Insecure {
		options = append(options, otlpmetricgrpc.WithInsecure())
	}
	if len(cfg.Headers) > 0 {
		options = append(options, otlpmetricgrpc.WithHeaders(cfg.Headers))
	}

	exporter, err := otlpmetricgrpc.New(ctx, options...)
	if err != nil {
		return nil, nil, err
	}
	res, err := newResource(cfg)
	if err != nil {
		return nil, nil, err
	}
	provider := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(exporter)),
	)
	return provider, provider.Shutdown, nil
}

func newResource(cfg config.ObservabilityConfig) (*resource.Resource, error) {
	return resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName(cfg.ServiceName),
			attribute.String("telemetry.sdk.language", "go"),
		),
	)
}

func shutdownAll(shutdowns ...func(context.Context) error) func(context.Context) error {
	return func(ctx context.Context) error {
		var result error
		for _, shutdown := range shutdowns {
			if shutdown == nil {
				continue
			}
			result = errors.Join(result, shutdown(ctx))
		}
		return result
	}
}

func StartSpan(ctx context.Context, name string, attrs ...attribute.KeyValue) (context.Context, trace.Span) {
	return otel.Tracer(instrumentationName).Start(ctx, name, trace.WithAttributes(attrs...))
}

func EndSpan(span trace.Span, err error) {
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}
