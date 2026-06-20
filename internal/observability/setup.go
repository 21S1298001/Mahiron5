package observability

import (
	"context"
	"log/slog"
	"os"

	"github.com/21S1298001/Mahiron5/internal/config"
	"go.opentelemetry.io/contrib/bridges/otelslog"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

type SetupResult struct {
	LogStore *LogStore
	Shutdown func(context.Context) error
}

func Setup(ctx context.Context, cfg config.ObservabilityConfig, level slog.Leveler) SetupResult {
	store := NewLogStore(defaultLogCapacity)
	stderr := slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level})
	apiLogs := slog.NewTextHandler(store, &slog.HandlerOptions{Level: level})

	var shutdown func(context.Context) error
	var handlers []slog.Handler
	handlers = append(handlers, stderr, apiLogs)

	if cfg.Endpoint != "" && cfg.Logs.Enabled {
		handler, cleanup, err := newOTelLogHandler(ctx, cfg)
		if err != nil {
			slog.New(stderr).Warn("failed to initialize OTLP log exporter", "err", err)
		} else {
			handlers = append(handlers, handler)
			shutdown = cleanup
		}
	}

	slog.SetDefault(slog.New(newFanoutHandler(handlers...)))

	if shutdown == nil {
		shutdown = func(context.Context) error { return nil }
	}
	return SetupResult{LogStore: store, Shutdown: shutdown}
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

	res, err := resource.Merge(
		resource.Default(),
		resource.NewSchemaless(
			semconv.ServiceName(cfg.ServiceName),
			attribute.String("telemetry.sdk.language", "go"),
		),
	)
	if err != nil {
		return nil, nil, err
	}

	provider := log.NewLoggerProvider(
		log.WithResource(res),
		log.WithProcessor(log.NewBatchProcessor(exporter)),
	)
	handler := otelslog.NewHandler("github.com/21S1298001/Mahiron5", otelslog.WithLoggerProvider(provider))
	return handler, provider.Shutdown, nil
}
