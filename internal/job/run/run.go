package run

import (
	"context"
	"encoding/json"

	"github.com/21S1298001/mahiron/internal/util"
)

type JobInfo struct {
	ID   string
	Key  string
	Name string
}

type Result struct {
	Kind     string         `json:"kind"`
	Summary  string         `json:"summary,omitempty"`
	Counts   map[string]int `json:"counts,omitempty"`
	Items    []Item         `json:"items,omitempty"`
	Warnings []string       `json:"warnings,omitempty"`
}

type Item struct {
	Kind    string         `json:"kind"`
	Summary string         `json:"summary,omitempty"`
	Data    map[string]any `json:"data,omitempty"`
}

type Reporter interface {
	SetJobResult(Result)
}

var jobContextKey util.ContextKey[JobInfo]

var reporterContextKey util.ContextKey[Reporter]

func WithJob(ctx context.Context, info JobInfo) context.Context {
	return util.ContextWith(ctx, jobContextKey, info)
}

func JobInfoFromContext(ctx context.Context) (JobInfo, bool) {
	return util.ContextGet(ctx, jobContextKey)
}

func WithReporter(ctx context.Context, reporter Reporter) context.Context {
	if reporter == nil {
		return ctx
	}
	return util.ContextWith(ctx, reporterContextKey, reporter)
}

func Set(ctx context.Context, result Result) {
	reporter, _ := util.ContextGet(ctx, reporterContextKey)
	if reporter == nil {
		return
	}
	reporter.SetJobResult(result)
}

func Clone(result *Result) *Result {
	if result == nil {
		return nil
	}
	data, err := json.Marshal(result)
	if err != nil {
		copy := *result
		return &copy
	}
	var cloned Result
	if err := json.Unmarshal(data, &cloned); err != nil {
		copy := *result
		return &copy
	}
	return &cloned
}
