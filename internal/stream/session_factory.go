package stream

import (
	"context"
	"errors"
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
	source := streamSessionSource(lease)
	if lease.Session != nil {
		return lease.Session, lease.RouteType, source, nil
	}

	broadcast := lease.Broadcast
	if broadcast == nil {
		broadcast = NewBroadcast(lease.Source, func() { m.remove(key) })
	} else {
		if !broadcast.AddOnStop(func() { m.remove(key) }) {
			return nil, "", "", errors.New("broadcast stopped")
		}
	}

	session := NewChannelSession(ChannelSessionConfig{
		Channel:     channel,
		Broadcast:   broadcast,
		Descrambler: lease.Descrambler,
		EITUpdater:  m.eitUpdater,
		LogoUpdater: m.logoUpdater,
		OnStop:      func() { m.remove(key) },
		Type:        channelType,
	})
	return session, lease.RouteType, source, nil
}

func streamSessionSource(lease *SourceLease) string {
	if lease != nil && lease.Session != nil {
		return "remote"
	}
	return "local"
}
