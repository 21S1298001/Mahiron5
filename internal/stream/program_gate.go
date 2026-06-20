package stream

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/21S1298001/Mahiron5/internal/epg"
	"github.com/21S1298001/Mahiron5/internal/util"
)

var (
	programEventEndGrace        = time.Second
	programEventMissingFallback = 3 * time.Minute
	programEventStaleAfter      = 10 * time.Second
	programEventWatchInterval   = 3 * time.Second
)

type programEventGateProcessor struct {
	collector      EITCollector
	initialTimeout time.Duration
	networkID      uint16
	serviceID      uint16
	eventID        uint16
}

func (p programEventGateProcessor) Run(ctx context.Context, src io.Reader, dst io.Writer) error {
	if p.collector == nil {
		return ErrEITCollectorNotConfigured
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	eitInR, eitInW := io.Pipe()
	eitOutR, eitOutW := io.Pipe()
	defer eitInW.Close()
	defer eitOutR.Close()

	gate := newProgramEventGate(p.networkID, p.serviceID, p.eventID, p.initialTimeout, cancel)

	collectorDone := make(chan error, 1)
	go func() {
		err := p.collector.CollectEITPF(ctx, eitInR, eitOutW)
		_ = eitOutW.Close()
		_ = eitInR.Close()
		collectorDone <- err
	}()

	scannerDone := make(chan error, 1)
	go func() {
		scannerDone <- p.scanEITPF(ctx, eitOutR, gate)
	}()

	copyDone := make(chan error, 1)
	go func() {
		copyDone <- gate.copy(ctx, src, dst, eitInW)
	}()

	var result error
	for range 2 {
		select {
		case err := <-copyDone:
			cancel()
			_ = eitInW.Close()
			result = errors.Join(result, expectedNil(err))
		case err := <-scannerDone:
			cancel()
			_ = closeReader(src)
			result = errors.Join(result, expectedNil(err))
		case <-ctx.Done():
			_ = closeReader(src)
			_ = eitInW.Close()
			result = errors.Join(result, expectedNil(ctx.Err()))
		}
		if ctx.Err() != nil {
			break
		}
	}

	if err := <-collectorDone; err != nil && ctx.Err() == nil && !util.IsExpectedStreamCloseError(err) {
		result = errors.Join(result, err)
	}
	return result
}

func (p programEventGateProcessor) scanEITPF(ctx context.Context, src io.Reader, gate *programEventGate) error {
	scanner := bufio.NewScanner(src)
	scanner.Buffer(make([]byte, 64*1024), 4*1024*1024)
	for scanner.Scan() {
		var section epg.EITSection
		if err := json.Unmarshal(scanner.Bytes(), &section); err != nil {
			slog.Debug("failed to decode EITPF section for program gate", "err", err)
			continue
		}
		gate.observe(section)
	}
	if err := scanner.Err(); err != nil && ctx.Err() == nil && !util.IsExpectedStreamCloseError(err) {
		return err
	}
	return nil
}

type programEventGate struct {
	cancel         context.CancelFunc
	eventID        uint16
	lastDetectedAt time.Time
	mu             sync.RWMutex
	networkID      uint16
	ready          bool
	serviceID      uint16
	stopTimer      *time.Timer
}

func newProgramEventGate(networkID, serviceID, eventID uint16, initialTimeout time.Duration, cancel context.CancelFunc) *programEventGate {
	if initialTimeout <= 0 {
		initialTimeout = programEventMissingFallback
	}
	g := &programEventGate{
		cancel:    cancel,
		eventID:   eventID,
		networkID: networkID,
		serviceID: serviceID,
	}
	g.stopTimer = time.AfterFunc(initialTimeout, g.closeIfStale)
	return g
}

func (g *programEventGate) observe(section epg.EITSection) {
	if section.TableID != 0x4e || section.SectionNumber != 0 || section.ServiceID != g.serviceID || section.OriginalNetworkID != g.networkID || len(section.Events) == 0 {
		return
	}

	if section.Events[0].EventID == g.eventID {
		g.mu.Lock()
		g.ready = true
		g.lastDetectedAt = time.Now()
		if g.stopTimer != nil {
			g.stopTimer.Reset(programEventStaleAfter)
		}
		g.mu.Unlock()
		return
	}

	g.mu.Lock()
	if g.ready {
		if g.stopTimer != nil {
			g.stopTimer.Reset(programEventEndGrace)
		}
	}
	g.mu.Unlock()
}

func (g *programEventGate) copy(ctx context.Context, src io.Reader, dst io.Writer, eitDst io.Writer) error {
	buf := make([]byte, 32*1024)
	for {
		n, readErr := src.Read(buf)
		if n > 0 {
			chunk := buf[:n]
			if _, err := eitDst.Write(chunk); err != nil && !util.IsExpectedStreamCloseError(err) {
				return err
			}
			if g.isReady() {
				if _, err := dst.Write(chunk); err != nil {
					return err
				}
			}
		}
		if readErr != nil {
			return readErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
	}
}

func (g *programEventGate) isReady() bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.ready
}

func (g *programEventGate) closeIfStale() {
	g.mu.RLock()
	lastDetectedAt := g.lastDetectedAt
	g.mu.RUnlock()
	if !lastDetectedAt.IsZero() && time.Since(lastDetectedAt) < programEventStaleAfter {
		g.stopTimer.Reset(programEventWatchInterval)
		return
	}
	g.cancel()
}

func closeReader(r io.Reader) error {
	closer, ok := r.(io.Closer)
	if !ok {
		return nil
	}
	return closer.Close()
}

func expectedNil(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || util.IsExpectedStreamCloseError(err) {
		return nil
	}
	return err
}
