package api

import (
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/job"
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
	})

	if item.Status != apigen.JobItemStatusStandby {
		t.Fatalf("status = %q, want standby", item.Status)
	}
	if got, ok := item.NextRunAt.Get(); !ok || got != apigen.UnixtimeMS(3000) {
		t.Fatalf("nextRunAt = %v, %v; want 3000, true", got, ok)
	}
}
