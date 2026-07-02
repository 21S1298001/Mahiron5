package local

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/21S1298001/mahiron/internal/util"
	"github.com/21S1298001/mahiron/ts"
)

var (
	programEventEndGrace        = time.Second
	programEventMissingFallback = 3 * time.Minute
	programEventStaleAfter      = 10 * time.Second
	programEventWatchInterval   = 3 * time.Second
)

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

func (g *programEventGate) observe(eit *ts.EIT) {
	if eit == nil {
		return
	}
	if eit.TableID != 0x4e || eit.SectionNumber != 0 || eit.ServiceID != g.serviceID || eit.OriginalNetworkID != g.networkID || len(eit.Events) == 0 {
		return
	}

	if eit.Events[0].EventID == g.eventID {
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

func expectedNil(err error) error {
	if err == nil || errors.Is(err, io.EOF) || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || util.IsExpectedStreamCloseError(err) {
		return nil
	}
	return err
}
