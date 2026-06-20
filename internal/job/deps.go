package job

import (
	"context"
	"io"

	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/service"
)

// This package keeps job orchestration thin. Feature-specific details should
// live behind usecase packages such as internal/epg.

type Registry interface {
	Register(JobDefinition)
	EnqueueDefinition(JobDefinition) (string, error)
}

type EPGServiceStore interface {
	GetServices(context.Context) ([]*service.Service, error)
	SetEPGAttempt(context.Context, uint16, uint16, int64, string) error
	SetEPGSuccess(context.Context, uint16, uint16, int64) error
}

type EPGProgramStore interface {
	UpsertPrograms(context.Context, []*program.Program) error
	DeleteEndedBefore(context.Context, int64) error
	ReplaceServicePrograms(context.Context, uint16, uint16, int64, []*program.Program) error
}

type EPGStreamManager interface {
	HasSession(string, string) bool
	GetOrCreate(context.Context, string, string) (interface {
		CollectEITS(context.Context, io.Writer) error
		CollectEITPF(context.Context, io.Writer) error
	}, error)
}
