package observability

import (
	"context"

	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type FilteringTracerProvider struct {
	noop.TracerProvider
	delegate trace.TracerProvider
	excluded map[string]struct{}
}

func NewFilteringTracerProvider(delegate trace.TracerProvider, excluded []string) trace.TracerProvider {
	if delegate == nil {
		delegate = noop.NewTracerProvider()
	}
	excludedSet := make(map[string]struct{}, len(excluded))
	for _, name := range excluded {
		excludedSet[name] = struct{}{}
	}
	return FilteringTracerProvider{delegate: delegate, excluded: excludedSet}
}

func (p FilteringTracerProvider) Tracer(name string, opts ...trace.TracerOption) trace.Tracer {
	return filteringTracer{
		delegate: p.delegate.Tracer(name, opts...),
		excluded: p.excluded,
		noop:     noop.NewTracerProvider().Tracer(name, opts...),
	}
}

type filteringTracer struct {
	noop.Tracer
	delegate trace.Tracer
	excluded map[string]struct{}
	noop     trace.Tracer
}

func (t filteringTracer) Start(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if _, ok := t.excluded[spanName]; ok {
		return t.noop.Start(ctx, spanName, opts...)
	}
	return t.delegate.Start(ctx, spanName, opts...)
}

var StreamOperationNames = []string{
	"GetLogStream",
	"GetEventsStream",
	"GetServiceStream",
	"GetProgramStream",
	"GetChannelStream",
	"GetServiceStreamByChannel",
	"ChannelsTypeChannelServicesIDStreamHead",
	"ProgramsIDStreamHead",
	"ChannelsTypeChannelStreamHead",
	"ChannelsTypeChannelServicesIDStreamHead",
}
