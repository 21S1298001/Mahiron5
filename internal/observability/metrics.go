package observability

import (
	"context"
	"log/slog"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	MetricStreamSessionsActive   = "mahiron5.stream.sessions.active"
	MetricTunerDevices           = "mahiron5.tuner.devices"
	MetricTunerUsers             = "mahiron5.tuner.users"
	MetricJobs                   = "mahiron5.jobs"
	MetricEPGProgramsStored      = "mahiron5.epg.programs.stored"
	MetricEPGServicesStale       = "mahiron5.epg.services.stale"
	MetricEPGServicesFailed      = "mahiron5.epg.services.failed"
	MetricJobRuns                = "mahiron5.job.runs"
	MetricJobDuration            = "mahiron5.job.duration"
	MetricStreamSessionStarts    = "mahiron5.stream.session.starts"
	MetricStreamSessionDuration  = "mahiron5.stream.session.duration"
	MetricStreamBytes            = "mahiron5.stream.bytes"
	MetricStreamPackets          = "mahiron5.stream.packets"
	MetricStreamPacketErrors     = "mahiron5.stream.packet.errors"
	MetricStreamContinuityErrors = "mahiron5.stream.continuity_counter.errors"
	MetricTunerAcquireRequests   = "mahiron5.tuner.acquire.requests"
	MetricTunerAcquireDuration   = "mahiron5.tuner.acquire.duration"
	MetricTunerProcessStarts     = "mahiron5.tuner.process.starts"
	MetricTunerProcessExits      = "mahiron5.tuner.process.exits"
	MetricTunerProcessRestarts   = "mahiron5.tuner.process.restart_attempts"
	MetricTunerProcessUptime     = "mahiron5.tuner.process.uptime"
	MetricRemoteRequests         = "mahiron5.remote.requests"
	MetricRemoteDuration         = "mahiron5.remote.duration"
	MetricRemoteErrors           = "mahiron5.remote.errors"
	MetricDBOperationDuration    = "mahiron5.db.operation.duration"
	MetricDBOperationErrors      = "mahiron5.db.operation.errors"
	MetricEventsSubscribers      = "mahiron5.events.subscribers"
	MetricLogsSubscribers        = "mahiron5.logs.subscribers"
	MetricEventsPublished        = "mahiron5.events.published"
)

const (
	AttrEventResource attribute.Key = "event.resource"
	AttrEventType     attribute.Key = "event.type"
	AttrJobResult     attribute.Key = "job.result"
	AttrOperation     attribute.Key = "operation"
	AttrResult        attribute.Key = "result"
	AttrSource        attribute.Key = "source"
	AttrState         attribute.Key = "state"
	AttrTunerIndex    attribute.Key = "tuner.index"
	AttrTunerName     attribute.Key = "tuner.name"
)

var jobMetrics struct {
	runs                   metric.Int64Counter
	duration               metric.Int64Histogram
	streamSessionStarts    metric.Int64Counter
	streamSessionDuration  metric.Int64Histogram
	streamBytes            metric.Int64Counter
	streamPackets          metric.Int64Counter
	streamPacketErrors     metric.Int64Counter
	streamContinuityErrors metric.Int64Counter
	tunerAcquireRequests   metric.Int64Counter
	tunerAcquireDuration   metric.Int64Histogram
	tunerProcessStarts     metric.Int64Counter
	tunerProcessExits      metric.Int64Counter
	tunerProcessRestarts   metric.Int64Counter
	remoteRequests         metric.Int64Counter
	remoteDuration         metric.Int64Histogram
	remoteErrors           metric.Int64Counter
	dbOperationDuration    metric.Int64Histogram
	dbOperationErrors      metric.Int64Counter
	eventsPublished        metric.Int64Counter
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
	streamBytes, err := meter.Int64Counter(MetricStreamBytes, metric.WithUnit("By"))
	if err != nil {
		slog.Warn("failed to create stream bytes metric", "err", err)
	}
	streamPackets, err := meter.Int64Counter(MetricStreamPackets)
	if err != nil {
		slog.Warn("failed to create stream packets metric", "err", err)
	}
	streamPacketErrors, err := meter.Int64Counter(MetricStreamPacketErrors)
	if err != nil {
		slog.Warn("failed to create stream packet errors metric", "err", err)
	}
	streamContinuityErrors, err := meter.Int64Counter(MetricStreamContinuityErrors)
	if err != nil {
		slog.Warn("failed to create stream continuity counter errors metric", "err", err)
	}
	tunerProcessStarts, err := meter.Int64Counter(MetricTunerProcessStarts)
	if err != nil {
		slog.Warn("failed to create tuner process starts metric", "err", err)
	}
	tunerProcessExits, err := meter.Int64Counter(MetricTunerProcessExits)
	if err != nil {
		slog.Warn("failed to create tuner process exits metric", "err", err)
	}
	tunerProcessRestarts, err := meter.Int64Counter(MetricTunerProcessRestarts)
	if err != nil {
		slog.Warn("failed to create tuner process restart attempts metric", "err", err)
	}
	remoteRequests, err := meter.Int64Counter(MetricRemoteRequests)
	if err != nil {
		slog.Warn("failed to create remote requests metric", "err", err)
	}
	remoteDuration, err := meter.Int64Histogram(MetricRemoteDuration, metric.WithUnit("ms"))
	if err != nil {
		slog.Warn("failed to create remote duration metric", "err", err)
	}
	remoteErrors, err := meter.Int64Counter(MetricRemoteErrors)
	if err != nil {
		slog.Warn("failed to create remote errors metric", "err", err)
	}
	dbOperationDuration, err := meter.Int64Histogram(MetricDBOperationDuration, metric.WithUnit("ms"))
	if err != nil {
		slog.Warn("failed to create DB operation duration metric", "err", err)
	}
	dbOperationErrors, err := meter.Int64Counter(MetricDBOperationErrors)
	if err != nil {
		slog.Warn("failed to create DB operation errors metric", "err", err)
	}
	eventsPublished, err := meter.Int64Counter(MetricEventsPublished)
	if err != nil {
		slog.Warn("failed to create events published metric", "err", err)
	}
	jobMetrics.runs = runs
	jobMetrics.duration = duration
	jobMetrics.streamSessionStarts = streamSessionStarts
	jobMetrics.streamSessionDuration = streamSessionDuration
	jobMetrics.streamBytes = streamBytes
	jobMetrics.streamPackets = streamPackets
	jobMetrics.streamPacketErrors = streamPacketErrors
	jobMetrics.streamContinuityErrors = streamContinuityErrors
	jobMetrics.tunerAcquireRequests = tunerAcquireRequests
	jobMetrics.tunerAcquireDuration = tunerAcquireDuration
	jobMetrics.tunerProcessStarts = tunerProcessStarts
	jobMetrics.tunerProcessExits = tunerProcessExits
	jobMetrics.tunerProcessRestarts = tunerProcessRestarts
	jobMetrics.remoteRequests = remoteRequests
	jobMetrics.remoteDuration = remoteDuration
	jobMetrics.remoteErrors = remoteErrors
	jobMetrics.dbOperationDuration = dbOperationDuration
	jobMetrics.dbOperationErrors = dbOperationErrors
	jobMetrics.eventsPublished = eventsPublished
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

func RecordStreamPacket(ctx context.Context, channelType, channelID string, bytes int64) {
	attrs := metric.WithAttributes(AttrChannelType.String(channelType), AttrChannelID.String(channelID))
	if jobMetrics.streamPackets != nil {
		jobMetrics.streamPackets.Add(ctx, 1, attrs)
	}
	if jobMetrics.streamBytes != nil && bytes > 0 {
		jobMetrics.streamBytes.Add(ctx, bytes, attrs)
	}
}

func RecordStreamPacketError(ctx context.Context, channelType, channelID, result string) {
	if jobMetrics.streamPacketErrors == nil {
		return
	}
	jobMetrics.streamPacketErrors.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
		AttrResult.String(result),
	))
}

func RecordStreamContinuityCounterError(ctx context.Context, channelType, channelID string) {
	if jobMetrics.streamContinuityErrors == nil {
		return
	}
	jobMetrics.streamContinuityErrors.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
	))
}

func RecordTunerProcessStart(ctx context.Context, channelType, channelID, result string) {
	if jobMetrics.tunerProcessStarts == nil {
		return
	}
	jobMetrics.tunerProcessStarts.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
		AttrResult.String(result),
	))
}

func RecordTunerProcessExit(ctx context.Context, channelType, channelID, result string) {
	if jobMetrics.tunerProcessExits == nil {
		return
	}
	jobMetrics.tunerProcessExits.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
		AttrResult.String(result),
	))
}

func RecordTunerProcessRestartAttempt(ctx context.Context, channelType, channelID string) {
	if jobMetrics.tunerProcessRestarts == nil {
		return
	}
	jobMetrics.tunerProcessRestarts.Add(ctx, 1, metric.WithAttributes(
		AttrChannelType.String(channelType),
		AttrChannelID.String(channelID),
	))
}

func RecordRemoteOperation(ctx context.Context, operation, result string, durationMS int64) {
	attrs := metric.WithAttributes(AttrOperation.String(operation), AttrResult.String(result))
	if jobMetrics.remoteRequests != nil {
		jobMetrics.remoteRequests.Add(ctx, 1, attrs)
	}
	if jobMetrics.remoteDuration != nil && durationMS >= 0 {
		jobMetrics.remoteDuration.Record(ctx, durationMS, attrs)
	}
	if jobMetrics.remoteErrors != nil && result != "success" {
		jobMetrics.remoteErrors.Add(ctx, 1, attrs)
	}
}

func RecordDBOperation(ctx context.Context, operation string, durationMS int64, err error) {
	attrs := metric.WithAttributes(AttrOperation.String(operation))
	if jobMetrics.dbOperationDuration != nil && durationMS >= 0 {
		jobMetrics.dbOperationDuration.Record(ctx, durationMS, attrs)
	}
	if jobMetrics.dbOperationErrors != nil && err != nil {
		jobMetrics.dbOperationErrors.Add(ctx, 1, attrs)
	}
}

func RecordEventPublished(ctx context.Context, resource, typ string) {
	if jobMetrics.eventsPublished == nil {
		return
	}
	jobMetrics.eventsPublished.Add(ctx, 1, metric.WithAttributes(
		AttrEventResource.String(resource),
		AttrEventType.String(typ),
	))
}
