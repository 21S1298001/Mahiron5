package stream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/internal/config"
	"github.com/21S1298001/Mahiron5/internal/epg"
	"github.com/21S1298001/Mahiron5/internal/program"
	"github.com/21S1298001/Mahiron5/internal/util"
	"github.com/21S1298001/Mahiron5/ts"
)

type ChannelSession struct {
	broadcast      *Broadcast
	channel        string
	descrambler    Descrambler
	eitCollector   EITCollector
	filter         ServiceFilter
	flows          *FlowRegistry
	logoPiggyback  *LogoPiggyback
	mu             sync.Mutex
	scanner        ServiceScanner
	stopped        bool
	typ            string
	sharedTSEngine bool
	rawEngine      *packetEngine
	decodedEngine  *packetEngine
	eitUpdater     EITSectionUpdater
	logoUpdater    LogoUpdater
	sectionCancel  context.CancelFunc
	sectionQueue   chan ts.Section
}

type ChannelSessionConfig struct {
	Channel        string
	ChannelConfig  *config.ChannelConfig
	Broadcast      *Broadcast
	Descrambler    Descrambler
	EITCollector   EITCollector
	EITUpdater     EITSectionUpdater
	Filter         ServiceFilter
	LogoPiggyback  *LogoPiggyback
	LogoUpdater    LogoUpdater
	OnStop         func()
	Scanner        ServiceScanner
	Type           string
	SharedTSEngine bool
}

func NewChannelSession(config ChannelSessionConfig) *ChannelSession {
	session := &ChannelSession{
		broadcast:      config.Broadcast,
		channel:        config.Channel,
		descrambler:    config.Descrambler,
		eitCollector:   config.EITCollector,
		filter:         config.Filter,
		logoPiggyback:  config.LogoPiggyback,
		scanner:        config.Scanner,
		typ:            config.Type,
		sharedTSEngine: config.SharedTSEngine,
		eitUpdater:     config.EITUpdater,
		logoUpdater:    config.LogoUpdater,
	}
	if config.SharedTSEngine {
		sectionCtx, sectionCancel := context.WithCancel(context.Background())
		session.sectionCancel = sectionCancel
		session.sectionQueue = make(chan ts.Section, sectionSubscriberBuffer)
		go session.runSharedSectionUpdates(sectionCtx)
		session.rawEngine = newPacketEngine(config.Broadcast.SubscribeRaw, config.OnStop, session.observeSharedSection)
		session.decodedEngine = newPacketEngine(session.subscribeDecodedMux, nil)
	}
	if !config.SharedTSEngine {
		session.flows = NewFlowRegistry(session.broadcast.SubscribeRaw, session.pipelineProcessors, config.OnStop)
	}
	return session
}

func (s *ChannelSession) RawStream(ctx context.Context, dst io.Writer) error {
	return s.ChannelStream(ctx, false, dst)
}

func (s *ChannelSession) ChannelStream(ctx context.Context, decode bool, dst io.Writer) error {
	if s.sharedTSEngine {
		return s.attachEngine(ctx, decode, 0, false, dst)
	}
	key := PipelineKey{
		ChannelType: s.typ,
		ChannelID:   s.channel,
		Kind:        PipelineChannelStream,
		Decode:      decode,
	}
	return s.attachPipeline(ctx, key, dst)
}

func (s *ChannelSession) ServiceStream(ctx context.Context, serviceID uint16, decode bool, dst io.Writer) error {
	if s.sharedTSEngine {
		return s.attachEngine(ctx, decode, serviceID, true, dst)
	}
	key := PipelineKey{
		ChannelType: s.typ,
		ChannelID:   s.channel,
		Kind:        PipelineServiceStream,
		ServiceID:   serviceID,
		Decode:      decode,
	}
	return s.attachPipeline(ctx, key, dst)
}

func (s *ChannelSession) ProgramStream(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	if s.sharedTSEngine {
		return s.programStreamShared(ctx, p, decode, dst)
	}
	key := PipelineKey{
		ChannelType:  s.typ,
		ChannelID:    s.channel,
		Kind:         PipelineProgramStream,
		NetworkID:    p.NetworkID,
		ServiceID:    p.ServiceID,
		EventID:      p.EventID,
		EventTimeout: programEventTimeout(p.StartAt, p.Duration),
		Decode:       decode,
	}
	return s.attachPipeline(ctx, key, dst)
}

func (s *ChannelSession) ScanServices(ctx context.Context) ([]ts.ServiceInfo, error) {
	if s.sharedTSEngine {
		scan := ts.NewServiceScan()
		err := s.broadcast.WithUser(ctx, func() error {
			return s.rawEngine.ObserveSections(ctx, func(section ts.Section) bool {
				switch section.TableID() {
				case ts.TableIDPAT, ts.TableIDSDT0, ts.TableIDNIT0:
					return true
				default:
					return false
				}
			}, func(section ts.Section) error {
				scan.Observe(section)
				if scan.Complete() {
					return errScanComplete
				}
				return nil
			})
		})
		if errors.Is(err, errScanComplete) {
			return scan.Services(), nil
		}
		return scan.Services(), err
	}
	if s.scanner == nil {
		return nil, ErrServiceScannerNotConfigured
	}
	var services []ts.ServiceInfo
	err := NewStreamTaskRunner(s.broadcast).RunTask(ctx, func(ctx context.Context, src io.Reader) error {
		var err error
		services, err = s.scanner.ScanServices(ctx, src)
		return err
	})
	return services, err
}

func (s *ChannelSession) CollectEITS(ctx context.Context, observe func(*ts.EIT) error) error {
	if s.sharedTSEngine {
		return s.broadcast.WithUser(ctx, func() error { return s.observeEIT(ctx, ts.IsEITS, observe) })
	}
	if s.eitCollector == nil {
		return ErrEITCollectorNotConfigured
	}
	return NewStreamTaskRunner(s.broadcast).RunTask(ctx, func(ctx context.Context, src io.Reader) error {
		return s.eitCollector.CollectEITS(ctx, src, observe)
	})
}

func (s *ChannelSession) CollectEITPF(ctx context.Context, observe func(*ts.EIT) error) error {
	if s.sharedTSEngine {
		return s.broadcast.WithUser(ctx, func() error { return s.observeEIT(ctx, ts.IsEITPF, observe) })
	}
	if s.eitCollector == nil {
		return ErrEITCollectorNotConfigured
	}
	return NewStreamTaskRunner(s.broadcast).RunTask(ctx, func(ctx context.Context, src io.Reader) error {
		return s.eitCollector.CollectEITPF(ctx, src, observe)
	})
}

func (s *ChannelSession) ObserveLogos(ctx context.Context, observe func(*ts.LogoImage) error) error {
	if s.sharedTSEngine {
		return s.broadcast.WithUser(ctx, func() error {
			return s.rawEngine.ObserveSections(ctx, func(section ts.Section) bool {
				return section.TableID() == ts.TableIDCDT
			}, func(section ts.Section) error {
				cdt, err := ts.ParseCDT(section)
				if err != nil {
					return nil
				}
				image, err := ts.ParseCDTLogoImage(cdt)
				if err != nil {
					return nil
				}
				return observe(image)
			})
		})
	}
	if s.logoPiggyback == nil {
		return ErrLogoCollectorNotConfigured
	}
	return s.logoPiggyback.Observe(ctx, s.broadcast, observe)
}

func (s *ChannelSession) Stop(ctx context.Context) error {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return nil
	}
	s.stopped = true
	flows := s.flows
	broadcast := s.broadcast
	rawEngine := s.rawEngine
	decodedEngine := s.decodedEngine
	sectionCancel := s.sectionCancel
	s.mu.Unlock()

	if flows != nil {
		flows.Stop()
	}
	if decodedEngine != nil {
		decodedEngine.Stop()
	}
	if rawEngine != nil {
		rawEngine.Stop()
	}
	if sectionCancel != nil {
		sectionCancel()
	}

	var result error
	if broadcast != nil {
		result = errors.Join(result, broadcast.Stop(ctx))
	}
	return result
}

var errScanComplete = errors.New("service scan complete")

func (s *ChannelSession) attachEngine(ctx context.Context, decode bool, serviceID uint16, service bool, dst io.Writer) error {
	s.mu.Lock()
	stopped := s.stopped
	s.mu.Unlock()
	if stopped {
		return errors.New("channel session stopped")
	}
	engine := s.rawEngine
	if decode && s.descrambler != nil {
		engine = s.decodedEngine
	}
	return s.broadcast.WithUser(ctx, func() error {
		if service {
			return engine.SubscribeService(ctx, serviceID, dst)
		}
		return engine.SubscribeChannel(ctx, dst)
	})
}

func (s *ChannelSession) subscribeDecodedMux(ctx context.Context, dst io.Writer) error {
	if s.descrambler == nil {
		return s.rawEngine.SubscribeChannel(ctx, dst)
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	r, w := io.Pipe()
	rawDone := make(chan error, 1)
	go func() {
		rawDone <- s.rawEngine.SubscribeChannel(ctx, w)
		_ = w.Close()
	}()
	err := s.descrambler.Descramble(ctx, r, dst)
	_ = r.Close()
	cancel()
	rawErr := <-rawDone
	if err == nil || util.IsExpectedStreamCloseError(err) || errors.Is(err, context.Canceled) {
		err = nil
	}
	if rawErr == nil || util.IsExpectedStreamCloseError(rawErr) || errors.Is(rawErr, context.Canceled) {
		rawErr = nil
	}
	return errors.Join(err, rawErr)
}

func (s *ChannelSession) programStreamShared(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gate := newProgramEventGate(p.NetworkID, p.ServiceID, p.EventID, programEventTimeout(p.StartAt, p.Duration), cancel)
	observerAttached := make(chan struct{})
	observeDone := make(chan error, 1)
	go func() {
		observeDone <- s.rawEngine.observeSectionsPassive(ctx, func(section ts.Section) bool {
			return ts.IsEITPF(section.TableID())
		}, func(section ts.Section) error {
			eit, err := ts.ParseEIT(section)
			if err == nil {
				gate.observe(epg.EITSectionFromTS(eit))
			}
			return nil
		}, observerAttached)
	}()
	select {
	case <-observerAttached:
	case err := <-observeDone:
		return expectedNil(err)
	case <-ctx.Done():
		return expectedNil(ctx.Err())
	}

	r, w := io.Pipe()
	sourceDone := make(chan error, 1)
	go func() {
		sourceDone <- s.attachEngine(ctx, decode, p.ServiceID, true, w)
		_ = w.Close()
	}()
	err := runSharedProgramGate(r, dst, gate)
	_ = r.Close()
	cancel()
	sourceErr := <-sourceDone
	observeErr := <-observeDone
	return errors.Join(expectedNil(err), expectedNil(sourceErr), expectedNil(observeErr))
}

func runSharedProgramGate(src io.Reader, dst io.Writer, gate *programEventGate) error {
	packet := make([]byte, ts.PacketSize)
	var result error
	for {
		_, err := io.ReadFull(src, packet)
		if err != nil {
			result = expectedNil(err)
			break
		}
		if gate.isReady() {
			n, err := dst.Write(packet)
			if err == nil && n != len(packet) {
				err = io.ErrShortWrite
			}
			if err != nil {
				result = err
				break
			}
		}
	}
	return result
}

func (s *ChannelSession) observeEIT(ctx context.Context, accept func(byte) bool, observe func(*ts.EIT) error) error {
	return s.rawEngine.ObserveSections(ctx, func(section ts.Section) bool {
		return accept(section.TableID())
	}, func(section ts.Section) error {
		eit, err := ts.ParseEIT(section)
		if err != nil {
			return nil
		}
		return observe(eit)
	})
}

func (s *ChannelSession) observeSharedSection(section ts.Section) {
	select {
	case s.sectionQueue <- section:
	default:
		slog.Warn("shared TS section updater overflow", "type", s.typ, "channel", s.channel)
	}
}

func (s *ChannelSession) runSharedSectionUpdates(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case section := <-s.sectionQueue:
			s.updateSharedSection(ctx, section)
		}
	}
}

func (s *ChannelSession) updateSharedSection(ctx context.Context, section ts.Section) {
	if ts.IsEITPF(section.TableID()) && s.eitUpdater != nil {
		if eit, err := ts.ParseEIT(section); err == nil {
			if err := s.eitUpdater.UpsertEIT(ctx, eit); err != nil {
				slog.Error("failed to update shared EITPF", "type", s.typ, "channel", s.channel, "err", err)
			}
		}
	}
	if section.TableID() == ts.TableIDCDT && s.logoUpdater != nil {
		if cdt, err := ts.ParseCDT(section); err == nil {
			if image, err := ts.ParseCDTLogoImage(cdt); err == nil {
				if err := s.logoUpdater.UpsertLogoImage(ctx, image); err != nil {
					slog.Error("failed to update shared logo", "type", s.typ, "channel", s.channel, "err", err)
				}
			}
		}
	}
}

func (s *ChannelSession) attachPipeline(ctx context.Context, key PipelineKey, dst io.Writer) error {
	s.mu.Lock()
	stopped := s.stopped
	flows := s.flows
	s.mu.Unlock()
	if stopped {
		return errors.New("channel session stopped")
	}
	return s.broadcast.WithUser(ctx, func() error {
		return flows.Attach(ctx, key, dst)
	})
}

func (s *ChannelSession) pipelineProcessors(key PipelineKey) []Processor {
	processors := []Processor{}
	if key.Decode && s.descrambler != nil {
		processors = append(processors, descramblerProcessor{descrambler: s.descrambler})
	}
	if key.Kind == PipelineServiceStream || key.Kind == PipelineProgramStream {
		if s.filter == nil {
			processors = append(processors, errorProcessor{err: ErrServiceFilterNotConfigured})
			return processors
		}
		processors = append(processors, serviceFilterProcessor{
			filter:    s.filter,
			serviceID: key.ServiceID,
		})
	}
	if key.Kind == PipelineProgramStream {
		processors = append(processors, programEventGateProcessor{
			collector:      s.eitCollector,
			eventID:        key.EventID,
			initialTimeout: key.EventTimeout,
			networkID:      key.NetworkID,
			serviceID:      key.ServiceID,
		})
	}
	return processors
}

func programEventTimeout(startAt int64, duration int) time.Duration {
	timeout := time.Until(time.UnixMilli(startAt + int64(duration)))
	if duration == 1 {
		timeout += programEventMissingFallback
	}
	if timeout < 0 {
		return programEventMissingFallback
	}
	return timeout
}
