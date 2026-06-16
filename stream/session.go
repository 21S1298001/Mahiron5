package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/filter"
	"github.com/21S1298001/Mahiron5/processor"
	"github.com/21S1298001/Mahiron5/tuner"
	"github.com/21S1298001/Mahiron5/util"
)

type StreamManager struct {
	mu            sync.Mutex
	channels      config.ChannelsConfig
	deviceFactory DeviceFactory
	filter        ServiceFilter
	scanner       ServiceScanner
	sessions      map[sessionKey]*ChannelSession
	tunerManager  *tuner.TunerManager
}

type StreamManagerConfig struct {
	Channels      config.ChannelsConfig
	DeviceFactory DeviceFactory
	Filter        ServiceFilter
	Scanner       ServiceScanner
	TunerManager  *tuner.TunerManager
}

type sessionKey struct {
	channel string
	typ     string
}

func NewStreamManager(cfg StreamManagerConfig) *StreamManager {
	serviceFilter := cfg.Filter
	if serviceFilter == nil {
		serviceFilter = filter.NewServiceFilter()
	}
	scanner := cfg.Scanner
	if scanner == nil {
		scanner = processor.NewServiceScanner()
	}
	deviceFactory := cfg.DeviceFactory
	if deviceFactory == nil {
		deviceFactory = func(t *tuner.Tuner, channel *config.ChannelConfig) (TunerDevice, error) {
			return t.NewDevice(channel), nil
		}
	}
	return &StreamManager{
		channels:      cfg.Channels,
		deviceFactory: deviceFactory,
		filter:        serviceFilter,
		scanner:       scanner,
		sessions:      map[sessionKey]*ChannelSession{},
		tunerManager:  cfg.TunerManager,
	}
}

func (m *StreamManager) GetOrCreate(ctx context.Context, channelType, channel string) (*ChannelSession, error) {
	key := sessionKey{typ: channelType, channel: channel}

	m.mu.Lock()
	defer m.mu.Unlock()

	if session := m.sessions[key]; session != nil {
		return session, nil
	}

	channelConfig := m.findChannel(channelType, channel)
	if channelConfig == nil {
		return nil, ErrChannelNotFound
	}
	if channelConfig.IsDisabled != nil && *channelConfig.IsDisabled {
		return nil, ErrChannelNotFound
	}

	group := channelConfig.Type
	if len(channelConfig.TunerGroups) > 0 {
		group = channelConfig.TunerGroups[0]
	}

	t := m.tunerManager.GetTunerByGroup(group)
	if t == nil {
		return nil, ErrTunerNotFound
	}
	if t.Command() == "" {
		return nil, ErrUnsupportedTuner
	}

	device, err := m.deviceFactory(t, channelConfig)
	if err != nil {
		return nil, err
	}

	session := NewChannelSession(ChannelSessionConfig{
		Channel:       channel,
		ChannelConfig: channelConfig,
		Device:        device,
		Filter:        m.filter,
		OnStop:        func() { m.remove(key) },
		Scanner:       m.scanner,
		Type:          channelType,
	})
	m.sessions[key] = session
	return session, nil
}

func (m *StreamManager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	sessions := make([]*ChannelSession, 0, len(m.sessions))
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

func (m *StreamManager) findChannel(channelType, channel string) *config.ChannelConfig {
	for i := range m.channels {
		if m.channels[i].Type == channelType && m.channels[i].Channel == channel {
			return &m.channels[i]
		}
	}
	return nil
}

func (m *StreamManager) remove(key sessionKey) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.sessions, key)
}

var (
	ErrChannelNotFound  = errors.New("channel not found")
	ErrTunerNotFound    = errors.New("tuner not found")
	ErrUnsupportedTuner = errors.New("unsupported tuner")
)

type DeviceFactory func(*tuner.Tuner, *config.ChannelConfig) (TunerDevice, error)

type TunerDevice interface {
	Start(context.Context, io.Writer) error
	Stop(context.Context) error
	Done() <-chan struct{}
	Err() error
}

type ServiceFilter interface {
	FilterService(context.Context, uint16, io.Reader, io.Writer) error
}

type ServiceScanner interface {
	ScanServices(context.Context, io.Reader, io.Writer) error
}

type ChannelSession struct {
	channel       string
	channelConfig *config.ChannelConfig
	ctx           context.Context
	cancel        context.CancelFunc
	device        TunerDevice
	done          <-chan struct{}
	filter        ServiceFilter
	hub           *util.DynamicMultiWriter
	mu            sync.Mutex
	onStop        func()
	refs          int
	scanner       ServiceScanner
	started       bool
	stopped       bool
	typ           string
}

type ChannelSessionConfig struct {
	Channel       string
	ChannelConfig *config.ChannelConfig
	Device        TunerDevice
	Filter        ServiceFilter
	OnStop        func()
	Scanner       ServiceScanner
	Type          string
}

func NewChannelSession(config ChannelSessionConfig) *ChannelSession {
	return &ChannelSession{
		channel:       config.Channel,
		channelConfig: config.ChannelConfig,
		device:        config.Device,
		filter:        config.Filter,
		hub:           util.NewDynamicMultiWriter(),
		onStop:        config.OnStop,
		scanner:       config.Scanner,
		typ:           config.Type,
	}
}

func (s *ChannelSession) RawStream(ctx context.Context, dst io.Writer) error {
	if err := s.attach(dst); err != nil {
		return err
	}
	defer s.detach(dst)

	select {
	case <-ctx.Done():
		return nil
	case <-s.done:
		err := s.device.Err()
		if errors.Is(err, io.ErrClosedPipe) {
			return nil
		}
		return err
	}
}

func (s *ChannelSession) ServiceStream(ctx context.Context, serviceID uint16, dst io.Writer) error {
	r, w := io.Pipe()
	filterCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- s.filter.FilterService(filterCtx, serviceID, r, dst)
	}()

	if err := s.attach(w); err != nil {
		_ = r.Close()
		_ = w.Close()
		cancel()
		<-waitCh
		return err
	}
	defer s.detach(w)

	select {
	case <-ctx.Done():
	case <-s.done:
		if err := s.device.Err(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			slog.Error("channel session ended while filtering service", "type", s.typ, "channel", s.channel, "service", serviceID, "err", err)
		}
		_ = w.Close()
		if err := <-waitCh; err != nil {
			slog.Error("service filter exited", "type", s.typ, "channel", s.channel, "service", serviceID, "err", err)
		}
		return nil
	case err := <-waitCh:
		if err != nil {
			slog.Error("service filter exited", "type", s.typ, "channel", s.channel, "service", serviceID, "err", err)
			return err
		}
	}

	_ = r.Close()
	_ = w.Close()
	cancel()
	<-waitCh
	return nil
}

func (s *ChannelSession) ScanServices(ctx context.Context, dst io.Writer) error {
	r, w := io.Pipe()
	scannerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- s.scanner.ScanServices(scannerCtx, r, dst)
	}()

	if err := s.attach(w); err != nil {
		_ = r.Close()
		_ = w.Close()
		cancel()
		<-waitCh
		return err
	}
	defer s.detach(w)

	select {
	case <-ctx.Done():
	case <-s.done:
		if err := s.device.Err(); err != nil && !errors.Is(err, io.ErrClosedPipe) {
			slog.Error("channel session ended while scanning services", "type", s.typ, "channel", s.channel, "err", err)
		}
		_ = w.Close()
		if err := <-waitCh; err != nil {
			return err
		}
		return nil
	case err := <-waitCh:
		if err != nil {
			return err
		}
		return nil
	}

	_ = r.Close()
	_ = w.Close()
	cancel()
	return <-waitCh
}

func (s *ChannelSession) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	cancel := s.cancel
	device := s.device
	s.hub.Close()
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	var result error
	if device != nil {
		result = errors.Join(result, device.Stop(ctx))
	}

	if s.onStop != nil {
		s.onStop()
	}
	return result
}

func (s *ChannelSession) attach(dst io.Writer) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.stopped {
		return errors.New("channel session stopped")
	}
	s.refs++
	s.hub.Attach(dst)
	if err := s.startLocked(); err != nil {
		s.refs--
		s.hub.Detach(dst)
		return err
	}
	return nil
}

func (s *ChannelSession) detach(dst io.Writer) {
	s.mu.Lock()
	if s.refs > 0 {
		s.refs--
	}
	s.hub.Detach(dst)
	refs := s.refs
	s.mu.Unlock()

	if refs == 0 {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.Stop(ctx); err != nil {
			slog.Error("failed to stop channel session", "type", s.typ, "channel", s.channel, "err", err)
		}
	}
}

func (s *ChannelSession) startLocked() error {
	if s.started {
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	s.ctx = ctx
	s.cancel = cancel
	if err := s.device.Start(ctx, s.hub); err != nil {
		cancel()
		return err
	}
	s.done = s.device.Done()

	s.started = true
	return nil
}
