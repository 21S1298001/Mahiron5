package stream

import (
	"context"
	"testing"
	"time"

	"github.com/21S1298001/mahiron/internal/epg"
)

func TestProgramEventGateTracksTargetEvent(t *testing.T) {
	restoreProgramGateTimings(t)
	programEventEndGrace = 10 * time.Millisecond
	programEventStaleAfter = 5 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	gate := newProgramEventGate(1, 101, 10, time.Second, cancel)
	gate.observe(gateSection(1, 101, 9))
	if gate.isReady() {
		t.Fatal("gate became ready for a different event")
	}
	gate.observe(gateSection(1, 101, 10))
	if !gate.isReady() {
		t.Fatal("gate did not become ready for the target event")
	}
	gate.observe(gateSection(1, 101, 11))
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("gate did not close after the target event ended")
	}
}

func TestProgramEventGateClosesWhenEventNeverAppears(t *testing.T) {
	restoreProgramGateTimings(t)
	ctx, cancel := context.WithCancel(context.Background())
	_ = newProgramEventGate(1, 101, 10, 10*time.Millisecond, cancel)
	select {
	case <-ctx.Done():
	case <-time.After(100 * time.Millisecond):
		t.Fatal("gate did not close after its initial timeout")
	}
}

func restoreProgramGateTimings(t *testing.T) {
	t.Helper()
	endGrace := programEventEndGrace
	missingFallback := programEventMissingFallback
	staleAfter := programEventStaleAfter
	watchInterval := programEventWatchInterval
	t.Cleanup(func() {
		programEventEndGrace = endGrace
		programEventMissingFallback = missingFallback
		programEventStaleAfter = staleAfter
		programEventWatchInterval = watchInterval
	})
}

func gateSection(networkID, serviceID, eventID uint16) *epg.EITSection {
	return &epg.EITSection{
		OriginalNetworkID: networkID,
		ServiceID:         serviceID,
		TableID:           0x4e,
		SectionNumber:     0,
		Events:            []epg.EITEvent{{EventID: eventID}},
	}
}
