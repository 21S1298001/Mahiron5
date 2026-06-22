package stream

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/ts"
)

func TestLogoPiggybackUsesOneCollectorForMultipleObservers(t *testing.T) {
	collector := &countingLogoCollector{}
	piggyback := NewLogoPiggyback("BS", "BS01", collector, noopLogoUpdater{})
	source := &logoObserveSource{fakeLiveSourceForBroadcast: newFakeLiveSource()}
	broadcast := NewBroadcast(source, []BroadcastHook{piggyback.Hook}, nil)
	doneErr := errors.New("target complete")

	results := make(chan error, 2)
	for range 2 {
		go func() {
			results <- piggyback.Observe(t.Context(), broadcast, func(*ts.LogoImage) error { return doneErr })
		}()
	}
	deadline := time.Now().Add(time.Second)
	for {
		piggyback.mu.Lock()
		observers := len(piggyback.observers)
		piggyback.mu.Unlock()
		if observers == 2 && collector.count() == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("observers=%d collector calls=%d", observers, collector.count())
		}
		time.Sleep(time.Millisecond)
	}

	piggyback.notify(&ts.LogoImage{OriginalNetworkID: 4, LogoID: 12})
	for range 2 {
		select {
		case err := <-results:
			if !errors.Is(err, doneErr) {
				t.Fatalf("Observe() error = %v", err)
			}
		case <-time.After(time.Second):
			t.Fatal("observer did not finish")
		}
	}
	if got := collector.count(); got != 1 {
		t.Fatalf("collector calls = %d, want 1", got)
	}
}

type countingLogoCollector struct {
	mu    sync.Mutex
	calls int
}

func (c *countingLogoCollector) Collect(ctx context.Context, _ io.Reader, _ func(*ts.LogoImage) error) error {
	c.mu.Lock()
	c.calls++
	c.mu.Unlock()
	<-ctx.Done()
	return ctx.Err()
}

func (c *countingLogoCollector) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.calls
}

type noopLogoUpdater struct{}

func (noopLogoUpdater) UpsertLogoImage(context.Context, *ts.LogoImage) error { return nil }

type logoObserveSource struct {
	*fakeLiveSourceForBroadcast
}

func (s *logoObserveSource) WithUser(_ context.Context, run func() error) error { return run() }
