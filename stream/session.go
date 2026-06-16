package stream

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/tuner"
	"github.com/21S1298001/Mahiron5/util"
)

type StreamManager struct {
	mu             sync.Mutex
	channels       config.ChannelsConfig
	processFactory ProcessFactory
	sessions       map[sessionKey]*ChannelSession
	tunerManager   *tuner.TunerManager
}

type StreamManagerConfig struct {
	Channels       config.ChannelsConfig
	ProcessFactory ProcessFactory
	TunerManager   *tuner.TunerManager
}

type sessionKey struct {
	channel string
	typ     string
}

func NewStreamManager(config StreamManagerConfig) *StreamManager {
	factory := config.ProcessFactory
	if factory == nil {
		factory = RealProcessFactory{}
	}
	return &StreamManager{
		channels:       config.Channels,
		processFactory: factory,
		sessions:       map[sessionKey]*ChannelSession{},
		tunerManager:   config.TunerManager,
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
	if t.SourceCommand() == "" {
		return nil, ErrUnsupportedTuner
	}

	session := NewChannelSession(ChannelSessionConfig{
		Channel:        channel,
		OnStop:         func() { m.remove(key) },
		ProcessFactory: m.processFactory,
		Tuner:          t,
		Type:           channelType,
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

type ChannelSession struct {
	channel        string
	ctx            context.Context
	cancel         context.CancelFunc
	decoder        Process
	done           chan struct{}
	err            error
	hub            *util.DynamicMultiWriter
	mu             sync.Mutex
	onStop         func()
	processFactory ProcessFactory
	refs           int
	started        bool
	stopped        bool
	tuner          *tuner.Tuner
	tunerProcess   Process
	typ            string
}

type ChannelSessionConfig struct {
	Channel        string
	OnStop         func()
	ProcessFactory ProcessFactory
	Tuner          *tuner.Tuner
	Type           string
}

func NewChannelSession(config ChannelSessionConfig) *ChannelSession {
	factory := config.ProcessFactory
	if factory == nil {
		factory = RealProcessFactory{}
	}
	return &ChannelSession{
		channel:        config.Channel,
		hub:            util.NewDynamicMultiWriter(),
		onStop:         config.OnStop,
		processFactory: factory,
		tuner:          config.Tuner,
		typ:            config.Type,
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
		if errors.Is(s.err, io.ErrClosedPipe) {
			return nil
		}
		return s.err
	}
}

func (s *ChannelSession) ServiceStream(ctx context.Context, serviceID uint16, dst io.Writer) error {
	if err := s.processFactory.EnsureCommand("mirakc-arib"); err != nil {
		return fmt.Errorf("mirakc-arib is required for service filtering: %w", err)
	}

	r, w := io.Pipe()
	filter := s.processFactory.NewProcess(fmt.Sprintf("mirakc-arib filter-service --sid %d", serviceID))
	filter.Stdin(r)
	filter.Stdout(dst)

	if err := filter.Start(); err != nil {
		_ = r.Close()
		_ = w.Close()
		return err
	}

	if err := s.attach(w); err != nil {
		_ = r.Close()
		_ = w.Close()
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = filter.Stop(stopCtx)
		return err
	}
	defer s.detach(w)

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- filter.Wait()
	}()

	select {
	case <-ctx.Done():
	case <-s.done:
		if s.err != nil && !errors.Is(s.err, io.ErrClosedPipe) {
			slog.Error("channel session ended while filtering service", "type", s.typ, "channel", s.channel, "service", serviceID, "err", s.err)
		}
		_ = w.Close()
		if err := <-waitCh; err != nil {
			slog.Error("service filter exited", "type", s.typ, "channel", s.channel, "service", serviceID, "err", err)
		}
		return nil
	case err := <-waitCh:
		if err != nil {
			slog.Error("service filter exited", "type", s.typ, "channel", s.channel, "service", serviceID, "err", err)
		}
	}

	_ = r.Close()
	_ = w.Close()
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := filter.Stop(stopCtx); err != nil {
		return err
	}
	return nil
}

func (s *ChannelSession) ScanServices(ctx context.Context, dst io.Writer) error {
	if err := s.processFactory.EnsureCommand("mirakc-arib"); err != nil {
		return fmt.Errorf("mirakc-arib is required for service scanning: %w", err)
	}

	r, w := io.Pipe()
	scanner := s.processFactory.NewProcess("mirakc-arib scan-services")
	scanner.Stdin(r)
	scanner.Stdout(dst)

	if err := scanner.Start(); err != nil {
		_ = r.Close()
		_ = w.Close()
		return err
	}

	if err := s.attach(w); err != nil {
		_ = r.Close()
		_ = w.Close()
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = scanner.Stop(stopCtx)
		return err
	}
	defer s.detach(w)

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- scanner.Wait()
	}()

	select {
	case <-ctx.Done():
	case <-s.done:
		if s.err != nil && !errors.Is(s.err, io.ErrClosedPipe) {
			slog.Error("channel session ended while scanning services", "type", s.typ, "channel", s.channel, "err", s.err)
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
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := scanner.Stop(stopCtx); err != nil {
		return err
	}
	return nil
}

func (s *ChannelSession) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	cancel := s.cancel
	tunerProcess := s.tunerProcess
	decoder := s.decoder
	done := s.done
	s.hub.Close()
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}

	var result error
	if tunerProcess != nil {
		result = errors.Join(result, tunerProcess.Stop(ctx))
	}
	if decoder != nil {
		result = errors.Join(result, decoder.Stop(ctx))
	}
	if done != nil {
		select {
		case <-done:
			if s.err != nil && !errors.Is(s.err, io.ErrClosedPipe) {
				result = errors.Join(result, s.err)
			}
		case <-ctx.Done():
			result = errors.Join(result, ctx.Err())
		}
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
	s.done = make(chan struct{})
	s.tunerProcess = s.processFactory.NewProcess(s.tuner.SourceCommand())

	tunerOut, err := s.tunerProcess.StdoutPipe()
	if err != nil {
		cancel()
		return err
	}

	if decoderCommand := s.tuner.DecoderCommand(); decoderCommand != "" {
		s.decoder = s.processFactory.NewProcess(decoderCommand)
		s.decoder.Stdin(tunerOut)
		s.decoder.Stdout(s.hub)
		if err := s.decoder.Start(); err != nil {
			cancel()
			return err
		}
		if err := s.tunerProcess.Start(); err != nil {
			stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopCancel()
			_ = s.decoder.Stop(stopCtx)
			cancel()
			return err
		}
		go s.waitDecoded()
	} else {
		if err := s.tunerProcess.Start(); err != nil {
			cancel()
			return err
		}
		go s.copyRaw(tunerOut)
	}

	s.started = true
	return nil
}

func (s *ChannelSession) copyRaw(src io.Reader) {
	_, err := io.Copy(s.hub, src)
	if err == nil || errors.Is(err, io.ErrClosedPipe) {
		s.finish(nil)
		return
	}
	s.finish(err)
}

func (s *ChannelSession) waitDecoded() {
	err := s.decoder.Wait()
	if err == nil {
		err = s.tunerProcess.Wait()
	}
	if err == nil || errors.Is(err, io.ErrClosedPipe) {
		s.finish(nil)
		return
	}
	s.finish(err)
}

func (s *ChannelSession) finish(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.done == nil {
		return
	}
	select {
	case <-s.done:
		return
	default:
		s.err = err
		close(s.done)
	}
}
