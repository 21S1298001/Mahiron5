package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/observability"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/tuner"
	"github.com/21S1298001/Mahiron5/ts"
)

type StreamManager struct {
	mu             sync.Mutex
	eitUpdater     EITSectionUpdater
	logoUpdater    LogoUpdater
	programUpdater ProgramUpdater
	remotes        map[string]*RemoteClient
	sessions       map[sessionKey]Session
	sessionTypes   map[sessionKey]string
	sources        *SourcePool
}

type StreamManagerConfig struct {
	Channels           config.ChannelsConfig
	DescramblerFactory DescramblerFactory
	EITUpdater         EITSectionUpdater
	Remotes            config.RemotesConfig
	LogoUpdater        LogoUpdater
	ProgramUpdater     ProgramUpdater
	TunerManager       TunerManager
}

type sessionKey struct {
	channel string
	typ     string
}

type Session interface {
	ChannelStream(context.Context, bool, io.Writer) error
	ProgramStream(context.Context, *program.Program, bool, io.Writer) error
	ServiceStream(context.Context, uint16, bool, io.Writer) error
	ScanServices(context.Context) ([]ts.ServiceInfo, error)
	CollectEIT(context.Context, func(*ts.EIT) error) error
	ObserveLogos(context.Context, func(*ts.LogoImage) error) error
	Stop(context.Context) error
}

func NewStreamManager(cfg StreamManagerConfig) *StreamManager {
	descramblerFactory := cfg.DescramblerFactory
	if descramblerFactory == nil {
		descramblerFactory = NewCommandDescrambler
	}
	remotes := make(map[string]*RemoteClient, len(cfg.Remotes))
	for _, remote := range cfg.Remotes {
		remotes[remote.Name] = newRemoteClient(remote)
	}
	return &StreamManager{
		eitUpdater:     cfg.EITUpdater,
		logoUpdater:    cfg.LogoUpdater,
		programUpdater: cfg.ProgramUpdater,
		remotes:        remotes,
		sessions:       map[sessionKey]Session{},
		sessionTypes:   map[sessionKey]string{},
		sources:        NewSourcePool(cfg.Channels, cfg.TunerManager, descramblerFactory, remotes),
	}
}

func (m *StreamManager) GetOrCreate(ctx context.Context, channelType, channel string) (Session, error) {
	return m.getOrCreate(ctx, channelType, channel, false)
}

func (m *StreamManager) GetOrCreateWait(ctx context.Context, channelType, channel string) (Session, error) {
	return m.getOrCreate(ctx, channelType, channel, true)
}

func (m *StreamManager) getOrCreate(ctx context.Context, channelType, channel string, wait bool) (session Session, err error) {
	ctx, span := observability.StartSpan(ctx, observability.SpanStreamGetOrCreate,
		observability.AttrChannelType.String(channelType),
		observability.AttrChannelID.String(channel),
		observability.AttrWait.Bool(wait),
	)
	defer func() { observability.EndSpan(span, err) }()

	key := sessionKey{typ: channelType, channel: channel}

	m.mu.Lock()
	defer m.mu.Unlock()

	if session := m.sessions[key]; session != nil {
		slog.Debug("reusing stream session", "type", channelType, "channel", channel)
		return session, nil
	}

	slog.Debug("creating stream session", "type", channelType, "channel", channel, "wait", wait)
	lease, err := m.sources.Acquire(ctx, channelType, channel, wait)
	if err != nil {
		slog.Debug("failed to acquire stream source", "type", channelType, "channel", channel, "wait", wait, "err", err)
		return nil, err
	}
	if lease.Session != nil {
		if remoteSession, ok := lease.Session.(*RemoteSession); ok {
			remoteSession.StartProgramEventSync(m.programUpdater)
		}
		m.sessions[key] = lease.Session
		m.sessionTypes[key] = lease.RouteType
		slog.Info("stream session created", "type", channelType, "channel", channel, "routeType", lease.RouteType, "source", "remote")
		return lease.Session, nil
	}
	broadcast := lease.Broadcast
	if broadcast == nil {
		broadcast = NewBroadcast(lease.Source, func() { m.remove(key) })
	} else {
		if !broadcast.AddOnStop(func() { m.remove(key) }) {
			return nil, errors.New("broadcast stopped")
		}
	}

	session = NewChannelSession(ChannelSessionConfig{
		Channel:     channel,
		Broadcast:   broadcast,
		Descrambler: lease.Descrambler,
		EITUpdater:  m.eitUpdater,
		LogoUpdater: m.logoUpdater,
		OnStop:      func() { m.remove(key) },
		Type:        channelType,
	})
	m.sessions[key] = session
	m.sessionTypes[key] = lease.RouteType
	slog.Info("stream session created", "type", channelType, "channel", channel, "routeType", lease.RouteType, "source", "local")
	return session, nil
}

func (m *StreamManager) HasSession(channelType, channel string) bool {
	key := sessionKey{typ: channelType, channel: channel}
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.sessions[key] != nil
}

func (m *StreamManager) ActiveSessionCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.sessions)
}

func (m *StreamManager) ActiveSessionCountByType(channelType string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	count := 0
	for _, typ := range m.sessionTypes {
		if typ == channelType {
			count++
		}
	}
	return count
}

func (m *StreamManager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	sessions := make([]Session, 0, len(m.sessions))
	for _, session := range m.sessions {
		sessions = append(sessions, session)
	}
	m.mu.Unlock()

	var result error
	for _, session := range sessions {
		if err := session.Stop(ctx); err != nil {
			result = errors.Join(result, err)
		}
	}
	return result
}

func (m *StreamManager) remove(key sessionKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, key)
	delete(m.sessionTypes, key)
	slog.Debug("stream session removed", "type", key.typ, "channel", key.channel)
}

var (
	ErrChannelNotFound            = errors.New("channel not found")
	ErrEITObservationUnsupported  = errors.New("EIT observation is not supported by remote sessions")
	ErrLogoObservationUnsupported = errors.New("logo observation is not supported by remote sessions")
	ErrTunerNotFound              = tuner.ErrTunerNotFound
	ErrUnsupportedTuner           = tuner.ErrUnsupportedTuner
	ErrTunerUnavailable           = tuner.ErrTunerUnavailable
)

type TunerManager interface {
	NewDeviceByType(string, *config.ChannelConfig) (TunerDevice, error)
}

type TunerAllocator interface {
	AcquireDevice(context.Context, string, *config.ChannelConfig, *config.ChannelConfig, bool) (TunerDevice, string, error)
}

type DecoderCommandProvider interface {
	DecoderCommandByType(string) string
}

type TunerDevice = tuner.Device

type EITSectionUpdater interface {
	UpsertEIT(ctx context.Context, eit *ts.EIT) error
}

type LogoUpdater interface {
	UpsertLogoImage(context.Context, *ts.LogoImage) error
}
