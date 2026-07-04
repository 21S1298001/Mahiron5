package runtimecontext

import (
	"context"

	"github.com/21S1298001/mahiron/internal/contextvalue"
)

var jobContextKey contextvalue.Key[JobInfo]

type JobInfo struct {
	ID   string
	Key  string
	Name string
}

func WithJob(ctx context.Context, info JobInfo) context.Context {
	return contextvalue.With(ctx, jobContextKey, info)
}

func JobFromContext(ctx context.Context) (JobInfo, bool) {
	return contextvalue.Get(ctx, jobContextKey)
}
