package api

import (
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/job"
	"github.com/21S1298001/mahiron/internal/job/run"
	apigen "github.com/21S1298001/mahiron/internal/web/api/gen"
)

func TestAPIJobItemIncludesStandbyAndNextRunAt(t *testing.T) {
	nextRunAt := time.UnixMilli(3000)
	item := apiJobItem(&job.Job{
		Key:        "retry-job",
		Name:       "Retry Job",
		ID:         "job-id",
		Status:     job.StatusStandby,
		CreatedAt:  time.UnixMilli(1000),
		UpdatedAt:  time.UnixMilli(2000),
		NextRunAt:  &nextRunAt,
		IsAborting: false,
		Result: &run.Result{
			Kind:    "service_scan",
			Summary: "GR/27: 1 services",
			Counts:  map[string]int{"services": 1},
			Items: []run.Item{{
				Kind:    "service",
				Summary: "NHK",
				Data: map[string]any{
					"name":      "NHK",
					"serviceId": 101,
				},
			}},
		},
	})

	if item.Status != apigen.JobItemStatusStandby {
		t.Fatalf("status = %q, want standby", item.Status)
	}
	if got, ok := item.NextRunAt.Get(); !ok || got != apigen.UnixtimeMS(3000) {
		t.Fatalf("nextRunAt = %v, %v; want 3000, true", got, ok)
	}
	result, ok := item.Result.Get()
	if !ok {
		t.Fatal("result was not set")
	}
	if result.Kind != "service_scan" {
		t.Fatalf("result kind = %q, want service_scan", result.Kind)
	}
	if got, ok := result.Summary.Get(); !ok || got != "GR/27: 1 services" {
		t.Fatalf("result summary = %q, %v", got, ok)
	}
	if got, ok := result.Counts.Get(); !ok || got["services"] != 1 {
		t.Fatalf("result counts = %#v, %v", got, ok)
	}
	if len(result.Items) != 1 || result.Items[0].Kind != "service" {
		t.Fatalf("result items = %#v", result.Items)
	}
}
