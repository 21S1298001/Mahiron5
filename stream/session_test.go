package stream

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/tuner"
)

func testManager(t *testing.T, devices *fakeTunerDeviceRecorder) *StreamManager {
	t.Helper()
	no := false
	return NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name:       "NHK",
				Type:       "GR",
				Channel:    "27",
				IsDisabled: &no,
			},
			{
				Name:       "BS",
				Type:       "BS",
				Channel:    "101",
				IsDisabled: &no,
			},
		},
		DeviceFactory: devices.NewDevice,
		Filter:        fakeFilter{},
		Scanner:       fakeScanner{},
		TunerManager: tuner.NewTunerManager(&tuner.TunerManagerConfig{
			TunersConfig: config.TunersConfig{
				{
					Name:    "Tuner1",
					Types:   []string{"GR", "BS"},
					Command: "tuner",
				},
			},
		}),
	})
}

func TestManagerSharesSessionsByTypeAndChannel(t *testing.T) {
	manager := testManager(t, &fakeTunerDeviceRecorder{})

	first, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	second, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	other, err := manager.GetOrCreate(context.Background(), "BS", "101")
	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Fatal("same type+channel should return the same session")
	}
	if first == other {
		t.Fatal("different type+channel should return a different session")
	}
}

func TestRawStream(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	manager := testManager(t, devices)
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := session.RawStream(context.Background(), &out); err != nil {
		t.Fatal(err)
	}

	if got, want := out.String(), "ts2"; got != want {
		t.Fatalf("raw stream = %q, want %q", got, want)
	}
	if got := devices.starts(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
}

func TestConcurrentRawStreamsStartOneTunerDevice(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	manager := testManager(t, devices)
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	var first bytes.Buffer
	var second bytes.Buffer
	wg.Add(2)
	go func() {
		defer wg.Done()
		if err := session.RawStream(context.Background(), &first); err != nil {
			t.Errorf("first stream: %v", err)
		}
	}()
	go func() {
		defer wg.Done()
		if err := session.RawStream(context.Background(), &second); err != nil {
			t.Errorf("second stream: %v", err)
		}
	}()
	wg.Wait()

	if got := devices.starts(); got != 1 {
		t.Fatalf("tuner device starts = %d, want 1", got)
	}
	if first.String() == "" || second.String() == "" {
		t.Fatalf("both subscribers should receive data: first=%q second=%q", first.String(), second.String())
	}
}

func TestServiceStreamAndScanAttachToSharedSession(t *testing.T) {
	devices := &fakeTunerDeviceRecorder{}
	manager := testManager(t, devices)
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	var serviceOut bytes.Buffer
	if err := session.ServiceStream(context.Background(), 1024, &serviceOut); err != nil {
		t.Fatal(err)
	}
	if got, want := serviceOut.String(), "filtered:ts2"; got != want {
		t.Fatalf("service stream = %q, want %q", got, want)
	}

	session, err = manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}
	var scanOut bytes.Buffer
	if err := session.ScanServices(context.Background(), &scanOut); err != nil {
		t.Fatal(err)
	}
	if got, want := scanOut.String(), "ts2"; got != want {
		t.Fatalf("scan stream = %q, want %q", got, want)
	}
}

type fakeTunerDeviceRecorder struct {
	mu      sync.Mutex
	devices []*fakeTunerDevice
}

func (r *fakeTunerDeviceRecorder) NewDevice(*tuner.Tuner, *config.ChannelConfig) (TunerDevice, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	device := &fakeTunerDevice{
		done: make(chan struct{}),
	}
	r.devices = append(r.devices, device)
	return device, nil
}

func (r *fakeTunerDeviceRecorder) starts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	count := 0
	for _, device := range r.devices {
		count += device.startCount()
	}
	return count
}

type fakeTunerDevice struct {
	done   chan struct{}
	err    error
	mu     sync.Mutex
	starts int
}

func (d *fakeTunerDevice) Start(_ context.Context, dst io.Writer) error {
	d.mu.Lock()
	d.starts++
	d.mu.Unlock()
	go func() {
		_, err := dst.Write([]byte("ts"))
		if err == nil {
			time.Sleep(20 * time.Millisecond)
			_, err = dst.Write([]byte("2"))
		}
		d.mu.Lock()
		d.err = err
		d.mu.Unlock()
		close(d.done)
	}()
	return nil
}

func (d *fakeTunerDevice) Stop(context.Context) error {
	return nil
}

func (d *fakeTunerDevice) Done() <-chan struct{} {
	return d.done
}

func (d *fakeTunerDevice) Err() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.err
}

func (d *fakeTunerDevice) startCount() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.starts
}

type fakeFilter struct{}

func (fakeFilter) FilterService(_ context.Context, _ uint16, src io.Reader, dst io.Writer) error {
	data, err := io.ReadAll(src)
	if err != nil {
		return err
	}
	_, err = dst.Write([]byte("filtered:" + string(data)))
	return err
}

type fakeScanner struct{}

func (fakeScanner) ScanServices(_ context.Context, src io.Reader, dst io.Writer) error {
	_, err := io.Copy(dst, src)
	return err
}
