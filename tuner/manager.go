package tuner

import (
	"context"
	"errors"
	"io"
	"slices"
	"sync"

	"github.com/21S1298001/Mahiron5/config"
)

type TunerManager struct {
	tuners     []*Tuner
	mu         sync.Mutex
	inUse      map[*Tuner]bool
	nextByType map[string]int
	changed    chan struct{}
}

type TunerManagerConfig struct{ TunersConfig config.TunersConfig }

func NewTunerManager(cfg *TunerManagerConfig) *TunerManager {
	tuners := make([]*Tuner, len(cfg.TunersConfig))
	for i, tunerConfig := range cfg.TunersConfig {
		tuners[i] = NewTuner(tunerConfig)
	}
	return &TunerManager{
		tuners:     tuners,
		inUse:      make(map[*Tuner]bool),
		nextByType: make(map[string]int),
		changed:    make(chan struct{}),
	}
}

func (tm *TunerManager) Shutdown(context.Context) error { return nil }

func (tm *TunerManager) GetTuner(name string) *Tuner {
	for _, item := range tm.tuners {
		if item.Name() == name {
			return item
		}
	}
	return nil
}

func (tm *TunerManager) GetTunerByType(channelType string) *Tuner {
	for _, item := range tm.tuners {
		if !item.IsDisabled() && slices.Contains(item.Groups(), channelType) {
			return item
		}
	}
	return nil
}

// NewDeviceByType reserves one physical tuner and returns a device that releases
// that reservation when it stops.
func (tm *TunerManager) NewDeviceByType(channelType string, channel *config.ChannelConfig) (Device, error) {
	device, _, err := tm.AcquireDevice(context.Background(), channelType, channel, false)
	return device, err
}

func (tm *TunerManager) AcquireDevice(ctx context.Context, channelType string, channel *config.ChannelConfig, wait bool) (Device, string, error) {
	for {
		tm.mu.Lock()
		found := false
		usable := false
		start := tm.nextByType[channelType]
		for offset := range len(tm.tuners) {
			index := (start + offset) % len(tm.tuners)
			item := tm.tuners[index]
			if item.IsDisabled() || !slices.Contains(item.Groups(), channelType) {
				continue
			}
			found = true
			if item.Command() == "" {
				continue
			}
			usable = true
			if tm.inUse[item] {
				continue
			}
			tm.inUse[item] = true
			tm.nextByType[channelType] = (index + 1) % len(tm.tuners)
			base := item.NewDevice(channel)
			managed := &managedDevice{Device: base, release: func() { tm.release(item) }}
			decoder := item.DecoderCommand()
			tm.mu.Unlock()
			return managed, decoder, nil
		}
		changed := tm.changed
		tm.mu.Unlock()

		if !found {
			return nil, "", ErrTunerNotFound
		}
		if !usable {
			return nil, "", ErrUnsupportedTuner
		}
		if !wait {
			return nil, "", ErrTunerUnavailable
		}
		select {
		case <-ctx.Done():
			return nil, "", ctx.Err()
		case <-changed:
		}
	}
}

func (tm *TunerManager) release(item *Tuner) {
	tm.mu.Lock()
	if tm.inUse[item] {
		delete(tm.inUse, item)
		close(tm.changed)
		tm.changed = make(chan struct{})
	}
	tm.mu.Unlock()
}

func (tm *TunerManager) DecoderCommandByType(channelType string) string {
	item := tm.GetTunerByType(channelType)
	if item == nil {
		return ""
	}
	return item.DecoderCommand()
}

func (tm *TunerManager) TunerCount() int { return len(tm.tuners) }

func (tm *TunerManager) TunerCountByType(channelType string) int {
	count := 0
	for _, item := range tm.tuners {
		if !item.IsDisabled() && slices.Contains(item.Groups(), channelType) {
			count++
		}
	}
	return count
}

func (tm *TunerManager) CountTunersByType() map[string]int {
	counts := make(map[string]int)
	for _, item := range tm.tuners {
		if item.IsDisabled() {
			continue
		}
		for _, group := range item.Groups() {
			counts[group]++
		}
	}
	return counts
}

type managedDevice struct {
	Device
	release func()
	once    sync.Once
}

func (d *managedDevice) Start(ctx context.Context, dst io.Writer) error {
	err := d.Device.Start(ctx, dst)
	if err != nil {
		d.releaseOnce()
	}
	return err
}

func (d *managedDevice) Stop(ctx context.Context) error {
	err := d.Device.Stop(ctx)
	d.releaseOnce()
	return err
}

func (d *managedDevice) releaseOnce() { d.once.Do(d.release) }

var (
	ErrTunerNotFound    = errors.New("tuner not found")
	ErrUnsupportedTuner = errors.New("unsupported tuner")
	ErrTunerUnavailable = errors.New("tuner unavailable")
)
