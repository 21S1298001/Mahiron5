package stream

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/internal/epg"
)

func TestProgramEventGateStartsAndStopsOnEITPF(t *testing.T) {
	restoreProgramGateTimings(t)
	programEventEndGrace = 10 * time.Millisecond
	programEventMissingFallback = 500 * time.Millisecond

	collector := &scriptedEITCollector{items: []scriptedEITItem{
		{delay: 5 * time.Millisecond, section: gateSection(1, 101, 9)},
		{delay: 20 * time.Millisecond, section: gateSection(1, 101, 10)},
		{delay: 55 * time.Millisecond, section: gateSection(1, 101, 11)},
	}}
	processor := programEventGateProcessor{
		collector:      collector,
		initialTimeout: 500 * time.Millisecond,
		networkID:      1,
		serviceID:      101,
		eventID:        10,
	}
	src := &slowChunkReader{
		delay:  8 * time.Millisecond,
		chunks: [][]byte{[]byte("before-1|"), []byte("before-2|"), []byte("during-1|"), []byte("during-2|"), []byte("after|")},
	}

	var out bytes.Buffer
	if err := processor.Run(context.Background(), src, &out); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got == "" || bytes.Contains([]byte(got), []byte("before")) {
		t.Fatalf("gated output = %q, want no data before target event", got)
	}
	if !bytes.Contains(out.Bytes(), []byte("during")) {
		t.Fatalf("gated output = %q, want target-event data", out.String())
	}
}

func TestProgramEventGateClosesWhenEventNeverAppears(t *testing.T) {
	restoreProgramGateTimings(t)
	programEventMissingFallback = 20 * time.Millisecond

	processor := programEventGateProcessor{
		collector:      &scriptedEITCollector{},
		initialTimeout: 20 * time.Millisecond,
		networkID:      1,
		serviceID:      101,
		eventID:        10,
	}
	src := &slowChunkReader{
		delay:  5 * time.Millisecond,
		chunks: [][]byte{[]byte("a"), []byte("b"), []byte("c"), []byte("d"), []byte("e"), []byte("f")},
	}

	var out bytes.Buffer
	if err := processor.Run(context.Background(), src, &out); err != nil {
		t.Fatal(err)
	}
	if out.String() != "" {
		t.Fatalf("gated output = %q, want empty", out.String())
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

func gateSection(networkID, serviceID, eventID uint16) epg.EITSection {
	return epg.EITSection{
		OriginalNetworkID: networkID,
		ServiceID:         serviceID,
		TableID:           0x4e,
		SectionNumber:     0,
		Events:            []epg.EITEvent{{EventID: eventID}},
	}
}

type scriptedEITItem struct {
	delay   time.Duration
	section epg.EITSection
}

type scriptedEITCollector struct {
	items []scriptedEITItem
}

func (c *scriptedEITCollector) CollectEITS(context.Context, io.Reader, io.Writer) error {
	return nil
}

func (c *scriptedEITCollector) CollectEITPF(ctx context.Context, src io.Reader, dst io.Writer) error {
	done := make(chan struct{})
	go func() {
		_, _ = io.Copy(io.Discard, src)
		close(done)
	}()
	encoder := json.NewEncoder(dst)
	for _, item := range c.items {
		select {
		case <-ctx.Done():
			<-done
			return nil
		case <-time.After(item.delay):
		}
		if err := encoder.Encode(item.section); err != nil {
			return err
		}
	}
	<-done
	return nil
}

type slowChunkReader struct {
	delay  time.Duration
	chunks [][]byte
	index  int
	mu     sync.Mutex
}

func (r *slowChunkReader) Read(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.index >= len(r.chunks) {
		return 0, io.EOF
	}
	time.Sleep(r.delay)
	chunk := r.chunks[r.index]
	r.index++
	return copy(p, chunk), nil
}

func (r *slowChunkReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.index = len(r.chunks)
	return nil
}
