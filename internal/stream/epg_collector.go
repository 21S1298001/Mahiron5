package stream

import (
	"context"

	"github.com/21S1298001/mahiron/ts"
)

type EPGCollectorAdapter struct {
	manager *StreamManager
}

func NewEPGCollectorAdapter(manager *StreamManager) *EPGCollectorAdapter {
	return &EPGCollectorAdapter{manager: manager}
}

func (a *EPGCollectorAdapter) HasSession(channelType, channel string) bool {
	return a.manager.HasSession(channelType, channel)
}

func (a *EPGCollectorAdapter) GetOrCreateWait(ctx context.Context, channelType, channel string) (interface {
	CollectEIT(context.Context, func(*ts.EIT) error) error
}, error) {
	return a.manager.GetOrCreateWait(ctx, channelType, channel)
}
