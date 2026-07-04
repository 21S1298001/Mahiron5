package stream

import (
	"context"

	"github.com/21S1298001/mahiron/internal/stream/local"
	"github.com/21S1298001/mahiron/internal/stream/remote"
	"github.com/21S1298001/mahiron/internal/stream/source"
)

// Both session implementations must satisfy the public Session interface.
var (
	_ Session = (*local.Session)(nil)
	_ Session = (*remote.Session)(nil)
)

// createSession acquires a source for the requested channel and builds the
// session that streams from it. A remote lease yields the remote session
// directly; a local lease produces (or reuses) a broadcast wrapped in a new
// channel session. The returned routeType and source describe the chosen path.
func (m *StreamManager) createSession(ctx context.Context, key sessionKey, channelType, channel string, wait bool) (Session, string, string, error) {
	lease, err := m.sources.Acquire(ctx, channelType, channel, wait)
	if err != nil {
		return nil, "", "", err
	}
	sourceLabel := streamSessionSource(lease)
	if lease.Remote != nil {
		return lease.Remote, lease.RouteType, sourceLabel, nil
	}

	broadcast := lease.Broadcast
	if broadcast == nil {
		broadcast = source.NewBroadcast(lease.Source, func() { m.remove(key) })
	} else {
		if !broadcast.AddOnStop(func() { m.remove(key) }) {
			return nil, "", "", source.ErrBroadcastStopped
		}
	}

	session := local.NewSession(local.Config{
		Channel:     channel,
		Broadcast:   broadcast,
		Descrambler: lease.Descrambler,
		EITUpdater:  m.eitUpdater,
		LogoUpdater: m.logoUpdater,
		OnStop:      func() { m.remove(key) },
		Type:        channelType,
	})
	return session, lease.RouteType, sourceLabel, nil
}

func streamSessionSource(lease *source.Lease) string {
	if lease != nil && lease.Remote != nil {
		return "remote"
	}
	return "local"
}
