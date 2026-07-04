package job

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/job/run"
)

type publishedEvent struct {
	resource string
	typ      string
	data     map[string]any
}

type fakeEventPublisher struct {
	events []publishedEvent
}

func (p *fakeEventPublisher) PublishJobEvent(typ string, data map[string]any) {
	p.events = append(p.events, publishedEvent{resource: "job", typ: typ, data: data})
}

func (p *fakeEventPublisher) PublishJobScheduleEvent(typ string, data map[string]any) {
	p.events = append(p.events, publishedEvent{resource: "job_schedule", typ: typ, data: data})
}

func TestJobEventDataIncludesMirakurunCompatibleFields(t *testing.T) {
	createdAt := time.UnixMilli(1000)
	startedAt := time.UnixMilli(2000)
	finishedAt := time.UnixMilli(3500)
	definition := &JobDefinition{IsRerunnable: true, RetryDelays: []time.Duration{2 * time.Second}}
	data := (&Job{
		Key:        "test-job",
		Name:       "Test Job",
		ID:         "job-id",
		Status:     StatusFinished,
		RetryCount: 1,
		HasFailed:  true,
		Error:      "failed",
		Result:     &run.Result{Kind: "service_scan", Summary: "scan completed"},
		CreatedAt:  createdAt,
		UpdatedAt:  finishedAt,
		StartedAt:  &startedAt,
		FinishedAt: &finishedAt,
		definition: definition,
	}).EventData()

	if data["key"] != "test-job" || data["name"] != "Test Job" || data["id"] != "job-id" || data["status"] != "finished" {
		t.Fatalf("job event data = %#v", data)
	}
	if data["retryCount"] != 1 || data["isAborting"] != false || data["hasSkipped"] != false || data["hasFailed"] != true || data["error"] != "failed" {
		t.Fatalf("job event state data = %#v", data)
	}
	if data["createdAt"] != int64(1000) || data["updatedAt"] != int64(3500) || data["startedAt"] != int64(2000) || data["finishedAt"] != int64(3500) || data["duration"] != 1500 {
		t.Fatalf("job event time data = %#v", data)
	}
	if data["isRerunnable"] != true || data["retryOnAbort"] != false || data["retryOnFail"] != true || data["retryMax"] != 1 || data["retryDelay"] != 2000 {
		t.Fatalf("job event retry data = %#v", data)
	}
	result, ok := data["result"].(*run.Result)
	if !ok || result.Kind != "service_scan" || result.Summary != "scan completed" {
		t.Fatalf("job event result data = %#v", data["result"])
	}
}

func TestScheduleInfoEventDataIncludesNestedJob(t *testing.T) {
	data := (ScheduleInfo{
		Key:      "schedule-key",
		Schedule: "5 6 * * *",
		JobKey:   "job-key",
		JobName:  "Job Name",
	}).EventData()

	job := data["job"].(map[string]any)
	if data["key"] != "schedule-key" || data["schedule"] != "5 6 * * *" || job["key"] != "job-key" || job["name"] != "Job Name" {
		t.Fatalf("schedule event data = %#v", data)
	}
}

func TestJobManagerPublishesJobLifecycleEvents(t *testing.T) {
	publisher := &fakeEventPublisher{}
	mgr, err := NewManager(Config{MaxHistory: 10}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })

	release := make(chan struct{})
	mgr.Register(JobDefinition{
		Key:  "test-job",
		Name: "Test Job",
		Handler: func(context.Context) error {
			<-release
			return nil
		},
	})

	id, err := mgr.Enqueue("test-job")
	if err != nil {
		t.Fatal(err)
	}
	close(release)
	waitJob(t, mgr, id)

	statuses := jobEventStatuses(publisher.events)
	if publisher.events[0].resource != "job" || publisher.events[0].typ != "create" || publisher.events[0].data["status"] != "queued" {
		t.Fatalf("first published event = %#v, want job create queued", publisher.events[0])
	}
	for _, want := range []string{"queued", "running", "finished"} {
		if !containsString(statuses, want) {
			t.Fatalf("published job statuses = %v, want %s", statuses, want)
		}
	}
}

func TestJobManagerPublishesRetryStandbyAndQueuedEvents(t *testing.T) {
	publisher := &fakeEventPublisher{}
	mgr, err := NewManager(Config{MaxHistory: 10}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })

	attempts := 0
	mgr.Register(JobDefinition{
		Key:         "retry-job",
		Name:        "Retry Job",
		RetryDelays: []time.Duration{10 * time.Millisecond},
		Handler: func(context.Context) error {
			attempts++
			if attempts == 1 {
				return errors.New("retry me")
			}
			return nil
		},
	})

	id, err := mgr.Enqueue("retry-job")
	if err != nil {
		t.Fatal(err)
	}
	waitJob(t, mgr, id)

	statuses := jobEventStatuses(publisher.events)
	for _, want := range []string{"standby", "queued"} {
		if !containsString(statuses, want) {
			t.Fatalf("published job statuses = %v, want %s", statuses, want)
		}
	}
}

func TestJobManagerPublishesJobScheduleCreateEvent(t *testing.T) {
	publisher := &fakeEventPublisher{}
	mgr, err := NewManager(Config{MaxHistory: 10}, publisher)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = mgr.Shutdown(context.Background()) })

	mgr.Register(JobDefinition{Key: "scheduled-job", Name: "Scheduled Job", Handler: func(context.Context) error { return nil }})
	if err := mgr.AddSchedule("scheduled-job", "5 6 * * *"); err != nil {
		t.Fatal(err)
	}

	if len(publisher.events) != 1 {
		t.Fatalf("published events = %#v", publisher.events)
	}
	got := publisher.events[0]
	job := got.data["job"].(map[string]any)
	if got.resource != "job_schedule" || got.typ != "create" || got.data["key"] != "scheduled-job" || got.data["schedule"] != "5 6 * * *" || job["name"] != "Scheduled Job" {
		t.Fatalf("published event = %#v", got)
	}
}

func jobEventStatuses(events []publishedEvent) []string {
	statuses := make([]string, 0, len(events))
	for _, event := range events {
		if event.resource == "job" {
			statuses = append(statuses, event.data["status"].(string))
		}
	}
	return statuses
}

func containsString(values []string, needle string) bool {
	for _, value := range values {
		if value == needle {
			return true
		}
	}
	return false
}
