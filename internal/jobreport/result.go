package jobreport

import (
	"context"
	"encoding/json"
)

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

type contextKey struct{}

func ContextWithReporter(ctx context.Context, reporter Reporter) context.Context {
	if reporter == nil {
		return ctx
	}
	return context.WithValue(ctx, contextKey{}, reporter)
}

func Set(ctx context.Context, result Result) {
	reporter, _ := ctx.Value(contextKey{}).(Reporter)
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
