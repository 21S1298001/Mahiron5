package stream

import (
	"context"
	"io"

	"github.com/21S1298001/Mahiron5/internal/program"
)

type APIStreamAdapter struct {
	manager *StreamManager
}

func NewAPIStreamAdapter(manager *StreamManager) *APIStreamAdapter {
	return &APIStreamAdapter{manager: manager}
}

func (a *APIStreamAdapter) GetOrCreate(ctx context.Context, channelType, channel string) (interface {
	ChannelStream(context.Context, bool, io.Writer) error
	ProgramStream(context.Context, *program.Program, bool, io.Writer) error
	ServiceStream(context.Context, uint16, bool, io.Writer) error
}, error) {
	return a.manager.GetOrCreate(ctx, channelType, channel)
}

func (a *APIStreamAdapter) ActiveSessionCount() int {
	return a.manager.ActiveSessionCount()
}
