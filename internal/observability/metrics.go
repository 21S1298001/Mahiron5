package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	MetricStreamSessionsActive  = "mahiron5.stream.sessions.active"
	MetricTunerDevices          = "mahiron5.tuner.devices"
	MetricTunerUsers            = "mahiron5.tuner.users"
	MetricJobs                  = "mahiron5.jobs"
	MetricEPGProgramsStored     = "mahiron5.epg.programs.stored"
	MetricEPGServicesStale      = "mahiron5.epg.services.stale"
	MetricEPGServicesFailed     = "mahiron5.epg.services.failed"
	MetricJobRuns               = "mahiron5.job.runs"
	MetricJobDuration           = "mahiron5.job.duration"
	MetricStreamSessionStarts   = "mahiron5.stream.session.starts"
	MetricStreamSessionDuration = "mahiron5.stream.session.duration"
	MetricTunerAcquireRequests  = "mahiron5.tuner.acquire.requests"
	MetricTunerAcquireDuration  = "mahiron5.tuner.acquire.duration"
)

const (
	AttrJobResult attribute.Key = "job.result"
	AttrResult    attribute.Key = "result"
	AttrSource    attribute.Key = "source"
	AttrState     attribute.Key = "state"
)

var jobMetrics struct {
	runs                  metric.Int64Counter
	duration              metric.Int64Histogram
	streamSessionStarts   metric.Int64Counter
	streamSessionDuration metric.Int64Histogram
	tunerAcquireRequests  metric.Int64Counter
	tunerAcquireDuration  metric.Int64Histogram
}

func initMetrics(provider metric.MeterProvider) {
	meter := provider.Meter(instrumentationName)
	runs, err := meter.Int64Counter(MetricJobRuns)
	if err != nil {
		slog.Warn("failed to create job run metric", "err", err)
	}
	duration, err := meter.Int64Histogram(MetricJobDuration, metric.WithUnit("ms"))
	if err != nil {
		slog.Warn("failed to create job duration metric", "err", err)
	}
	streamSessionStarts, err := meter.Int64Counter(MetricStreamSessionStarts)
	if err != nil {
		slog.Warn("failed to create stream session starts metric", "err", err)
	}
	streamSessionDuration, err := meter.Int64Histogram(MetricStreamSessionDuration, metric.WithUnit("ms"))
	if err != nil {
		slog.Warn("failed to create stream session duration metric", "err", err)
	}
	tunerAcquireRequests, err := meter.Int64Counter(MetricTunerAcquireRequests)
	if err != nil {
		slog.Warn("failed to create tuner acquire requests metric", "err", err)
	}
	tunerAcquireDuration, err := meter.Int64Histogram(MetricTunerAcquireDuration, metric.WithUnit("ms"))
	if err != nil {
		slog.Warn("failed to create tuner acquire duration metric", "err", err)
	}
	jobMetrics.runs = runs
	jobMetrics.duration = duration
	jobMetrics.streamSessionStarts = streamSessionStarts
	jobMetrics.streamSessionDuration = streamSessionDuration
	jobMetrics.tunerAcquireRequests = tunerAcquireRequests
	jobMetrics.tunerAcquireDuration = tunerAcquireDuration
}

func RecordJobRun(ctx context.Context, key, result string, durationMS int64) {
	attrs := metric.WithAttributes(AttrJobKey.String(key), AttrJobResult.String(result))
	if jobMetrics.runs != nil {
		jobMetrics.runs.Add(ctx, 1, attrs)
	}
	if jobMetrics.duration != nil && durationMS >= 0 {
		jobMetrics.duration.Record(ctx, durationMS, attrs)
	}
}

func RecordStreamSessionStart(ctx context.Context, channelType, routeType, source, result string) {
	if jobMetrics.streamSessionStarts == nil {
		return
	}
	jobMetrics.streamSessionStarts.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrRouteType.String(routeType),
		AttrSource.String(source),
		AttrResult.String(result),
	))
}

func RecordStreamSessionDuration(ctx context.Context, channelType, routeType, source string, durationMS int64) {
	if jobMetrics.streamSessionDuration == nil || durationMS < 0 {
		return
	}
	jobMetrics.streamSessionDuration.Record(ctx, durationMS, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrRouteType.String(routeType),
		AttrSource.String(source),
	))
}

func RecordTunerAcquire(ctx context.Context, channelType, result string, wait bool, durationMS int64) {
	attrs := metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrResult.String(result),
		AttrWait.Bool(wait),
	)
	if jobMetrics.tunerAcquireRequests != nil {
		jobMetrics.tunerAcquireRequests.Add(ctx, 1, attrs)
	}
	if jobMetrics.tunerAcquireDuration != nil && durationMS >= 0 {
		jobMetrics.tunerAcquireDuration.Record(ctx, durationMS, attrs)
	}
}
