package stream

import (
	"context"
	"io"
	"log/slog"
	"sync"

	"github.com/21S1298001/Mahiron5/internal/util"
	"github.com/21S1298001/Mahiron5/ts"
)

type LogoPiggyback struct {
	channel     string
	channelType string
	collector   LogoCollector
	updater     LogoUpdater
	mu          sync.Mutex
	nextID      uint64
	observers   map[uint64]*logoObserver
}

type logoObserver struct {
	callback func(*ts.LogoImage) error
	done     chan error
	mu       sync.Mutex
	active   bool
}

func NewLogoPiggyback(channelType, channel string, collector LogoCollector, updater LogoUpdater) *LogoPiggyback {
	if collector == nil || updater == nil {
		return nil
	}
	return &LogoPiggyback{
		channel:     channel,
		channelType: channelType,
		collector:   collector,
		updater:     updater,
		observers:   make(map[uint64]*logoObserver),
	}
}

// Observe starts the broadcast when necessary and receives images from the
// session's single piggyback collector.  Returning an error from observe ends
// only this observer; it does not stop collection for other session users.
func (p *LogoPiggyback) Observe(ctx context.Context, broadcast *Broadcast, observe func(*ts.LogoImage) error) error {
	observer := &logoObserver{callback: observe, done: make(chan error, 1), active: true}
	p.mu.Lock()
	id := p.nextID
	p.nextID++
	p.observers[id] = observer
	p.mu.Unlock()
	defer func() {
		p.mu.Lock()
		delete(p.observers, id)
		p.mu.Unlock()
	}()

	sourceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sourceDone := make(chan error, 1)
	go func() { sourceDone <- broadcast.Subscribe(sourceCtx, io.Discard) }()

	select {
	case err := <-observer.done:
		cancel()
		<-sourceDone
		return err
	case err := <-sourceDone:
		return err
	case <-ctx.Done():
		cancel()
		<-sourceDone
		return ctx.Err()
	}
}

func (p *LogoPiggyback) Hook(ctx context.Context, broadcast *Broadcast) {
	r, w := io.Pipe()
	go func() {
		slog.Debug("starting logo piggyback collection", "type", p.channelType, "channel", p.channel)
		defer r.Close()
		defer w.Close()
		defer slog.Debug("finished logo piggyback collection", "type", p.channelType, "channel", p.channel)

		done := make(chan error, 1)
		go func() {
			done <- broadcast.Tap(ctx, w)
		}()

		collectDone := make(chan error, 1)
		go func() {
			collectDone <- p.collector.Collect(ctx, r, func(image *ts.LogoImage) error {
				if err := p.updater.UpsertLogoImage(ctx, image); err != nil {
					slog.Error("failed to update logo", "type", p.channelType, "channel", p.channel, "networkId", image.OriginalNetworkID, "logoId", image.LogoID, "err", err)
					return nil
				}
				p.notify(image)
				return nil
			})
		}()
		if err := <-collectDone; err != nil && ctx.Err() == nil && !util.IsExpectedStreamCloseError(err) {
			slog.Error("failed to collect logos", "type", p.channelType, "channel", p.channel, "err", err)
		}
		if err := <-done; err != nil && ctx.Err() == nil && !util.IsExpectedStreamCloseError(err) {
			slog.Error("failed logo piggyback source", "type", p.channelType, "channel", p.channel, "err", err)
		}
	}()
}

func (p *LogoPiggyback) notify(image *ts.LogoImage) {
	p.mu.Lock()
	observers := make([]*logoObserver, 0, len(p.observers))
	for _, observer := range p.observers {
		observers = append(observers, observer)
	}
	p.mu.Unlock()
	for _, observer := range observers {
		observer.mu.Lock()
		if !observer.active {
			observer.mu.Unlock()
			continue
		}
		if err := observer.callback(image); err != nil {
			observer.active = false
			observer.done <- err
		}
		observer.mu.Unlock()
	}
}
