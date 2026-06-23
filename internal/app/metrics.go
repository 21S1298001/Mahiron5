package app

import (
	"context"
	"log/slog"
	"time"

	"github.com/21S1298001/Mahiron5/internal/job"
	"github.com/21S1298001/Mahiron5/internal/observability"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/service"
	"github.com/21S1298001/Mahiron5/internal/stream"
	"github.com/21S1298001/Mahiron5/internal/tuner"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

func registerRuntimeMetrics(
	provider metric.MeterProvider,
	streams *stream.StreamManager,
	tuners *tuner.TunerManager,
	jobs *job.JobManager,
	programs *program.ProgramManager,
	services *service.ServiceManager,
	epgStaleAfter int64,
) {
	if provider == nil {
		return
	}
	meter := provider.Meter("github.com/21S1298001/Mahiron5")
	streamSessions, err := meter.Int64ObservableGauge(observability.MetricStreamSessionsActive)
	if err != nil {
		slog.Warn("failed to create stream sessions metric", "err", err)
		return
	}
	tunerDevices, err := meter.Int64ObservableGauge(observability.MetricTunerDevices)
	if err != nil {
		slog.Warn("failed to create tuner devices metric", "err", err)
		return
	}
	tunerUsers, err := meter.Int64ObservableGauge(observability.MetricTunerUsers)
	if err != nil {
		slog.Warn("failed to create tuner users metric", "err", err)
		return
	}
	jobCount, err := meter.Int64ObservableGauge(observability.MetricJobs)
	if err != nil {
		slog.Warn("failed to create jobs metric", "err", err)
		return
	}
	epgPrograms, err := meter.Int64ObservableGauge(observability.MetricEPGProgramsStored)
	if err != nil {
		slog.Warn("failed to create EPG programs metric", "err", err)
		return
	}
	epgStale, err := meter.Int64ObservableGauge(observability.MetricEPGServicesStale)
	if err != nil {
		slog.Warn("failed to create stale EPG services metric", "err", err)
		return
	}
	epgFailed, err := meter.Int64ObservableGauge(observability.MetricEPGServicesFailed)
	if err != nil {
		slog.Warn("failed to create failed EPG services metric", "err", err)
		return
	}

	_, err = meter.RegisterCallback(func(ctx context.Context, observer metric.Observer) error {
		observer.ObserveInt64(streamSessions, int64(streams.ActiveSessionCount()))
		observeTunerMetrics(observer, tunerDevices, tunerUsers, tuners.Statuses())
		observeJobMetrics(observer, jobCount, jobs.GetJobs(), jobs.GetJobSchedules())
		observeEPGMetrics(ctx, observer, epgPrograms, epgStale, epgFailed, programs, services, epgStaleAfter)
		return nil
	}, streamSessions, tunerDevices, tunerUsers, jobCount, epgPrograms, epgStale, epgFailed)
	if err != nil {
		slog.Warn("failed to register runtime metrics callback", "err", err)
	}
}

func observeTunerMetrics(observer metric.Observer, devices, users metric.Int64ObservableGauge, statuses []tuner.Status) {
	counts := map[string]int64{
		"available": 0,
		"free":      0,
		"using":     0,
		"fault":     0,
	}
	var userCount int64
	for _, status := range statuses {
		if status.IsAvailable {
			counts["available"]++
		}
		if status.IsFree {
			counts["free"]++
		}
		if status.IsUsing {
			counts["using"]++
		}
		if status.IsFault {
			counts["fault"]++
		}
		userCount += int64(len(status.Users))
	}
	for state, count := range counts {
		observer.ObserveInt64(devices, count, metric.WithAttributes(observability.AttrState.String(state)))
	}
	observer.ObserveInt64(users, userCount)
}

func observeJobMetrics(observer metric.Observer, instrument metric.Int64ObservableGauge, jobs []*job.Job, schedules []job.ScheduleInfo) {
	counts := make(map[jobMetricKey]int64)
	for _, schedule := range schedules {
		for _, status := range []job.JobStatus{job.StatusQueued, job.StatusStandby, job.StatusRunning, job.StatusFinished} {
			counts[jobMetricKey{key: schedule.JobKey, status: string(status)}] = 0
		}
	}
	for _, item := range jobs {
		counts[jobMetricKey{key: item.Key, status: string(item.Status)}]++
	}
	for key, count := range counts {
		observer.ObserveInt64(instrument, count, metric.WithAttributes(
			observability.AttrJobKey.String(key.key),
			attribute.String("job.status", key.status),
		))
	}
}

func observeEPGMetrics(
	ctx context.Context,
	observer metric.Observer,
	programsInstrument, staleInstrument, failedInstrument metric.Int64ObservableGauge,
	programs *program.ProgramManager,
	services *service.ServiceManager,
	epgStaleAfter int64,
) {
	count, err := programs.Count(ctx)
	if err != nil {
		slog.Debug("failed to observe stored EPG programs metric", "err", err)
	} else {
		observer.ObserveInt64(programsInstrument, int64(count))
	}

	stale, failed, _, err := services.EPGSummary(ctx, epgStaleAfter, time.Now().UnixMilli())
	if err != nil {
		slog.Debug("failed to observe EPG service metrics", "err", err)
		return
	}
	observer.ObserveInt64(staleInstrument, int64(stale))
	observer.ObserveInt64(failedInstrument, int64(failed))
}

type jobMetricKey struct {
	key    string
	status string
}
