package stream

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/tuner"
)

type fakeProcessFactory struct {
	ensureErr error
	mu        sync.Mutex
	processes []*fakeProcess
}

func (f *fakeProcessFactory) EnsureCommand(name string) error {
	return f.ensureErr
}

func (f *fakeProcessFactory) NewProcess(command string) Process {
	f.mu.Lock()
	defer f.mu.Unlock()
	p := &fakeProcess{
		command: command,
		done:    make(chan struct{}),
	}
	f.processes = append(f.processes, p)
	return p
}

func (f *fakeProcessFactory) count(command string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	count := 0
	for _, p := range f.processes {
		if p.command == command {
			count++
		}
	}
	return count
}

type fakeProcess struct {
	command string
	done    chan struct{}
	err     error
	mu      sync.Mutex
	stdin   io.Reader
	stdout  io.Writer
	pipeW   *io.PipeWriter
}

func (p *fakeProcess) Stdin(r io.Reader) {
	p.stdin = r
}

func (p *fakeProcess) Stdout(w io.Writer) {
	p.stdout = w
}

func (p *fakeProcess) StdoutPipe() (io.ReadCloser, error) {
	r, w := io.Pipe()
	p.pipeW = w
	return r, nil
}

func (p *fakeProcess) Start() error {
	go func() {
		var err error
		switch {
		case strings.HasPrefix(p.command, "tuner"):
			err = p.writeTS()
		case strings.HasPrefix(p.command, "decoder"):
			err = p.copyWithPrefix("decoded:")
		case strings.HasPrefix(p.command, "mirakc-arib filter-service"):
			err = p.copyWithPrefix("filtered:")
		case p.command == "mirakc-arib scan-services":
			err = p.copyWithPrefix("")
		default:
			err = fmt.Errorf("unexpected command: %s", p.command)
		}
		p.finish(err)
	}()
	return nil
}

func (p *fakeProcess) Wait() error {
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *fakeProcess) Stop(ctx context.Context) error {
	if p.pipeW != nil {
		_ = p.pipeW.Close()
	}
	select {
	case <-p.done:
		p.mu.Lock()
		defer p.mu.Unlock()
		return p.err
	case <-ctx.Done():
		return nil
	}
}

func (p *fakeProcess) finish(err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	select {
	case <-p.done:
		return
	default:
		p.err = err
		close(p.done)
	}
}

func (p *fakeProcess) writeTS() error {
	if p.pipeW == nil {
		return nil
	}
	_, err := p.pipeW.Write([]byte("ts"))
	if err != nil {
		_ = p.pipeW.Close()
		return err
	}
	time.Sleep(20 * time.Millisecond)
	_, err = p.pipeW.Write([]byte("2"))
	_ = p.pipeW.Close()
	return err
}

func (p *fakeProcess) copyWithPrefix(prefix string) error {
	if p.stdin == nil || p.stdout == nil {
		return nil
	}
	data, err := io.ReadAll(p.stdin)
	if err != nil {
		return err
	}
	_, err = p.stdout.Write([]byte(prefix + string(data)))
	return err
}

func testManager(factory *fakeProcessFactory, decoder string) *StreamManager {
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
		ProcessFactory: factory,
		TunerManager: tuner.NewTunerManager(&tuner.TunerManagerConfig{
			TunersConfig: config.TunersConfig{
				{
					Name:    "Tuner1",
					Types:   []string{"GR", "BS"},
					Command: "tuner",
					Decoder: decoder,
				},
			},
		}),
	})
}

func TestManagerSharesSessionsByTypeAndChannel(t *testing.T) {
	manager := testManager(&fakeProcessFactory{}, "")

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

func TestRawStreamWithoutDecoder(t *testing.T) {
	factory := &fakeProcessFactory{}
	manager := testManager(factory, "")
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
	if got := factory.count("decoder"); got != 0 {
		t.Fatalf("decoder process count = %d, want 0", got)
	}
}

func TestRawStreamReplacesMirakurunCommandTemplate(t *testing.T) {
	no := false
	factory := &fakeProcessFactory{}
	manager := NewStreamManager(StreamManagerConfig{
		Channels: config.ChannelsConfig{
			{
				Name:        "NHK",
				Type:        "GR",
				Channel:     "27",
				CommandVars: map[string]any{"freq": 12345, "polarity": "H", "extra-args": "--foo"},
				IsDisabled:  &no,
			},
		},
		ProcessFactory: factory,
		TunerManager: tuner.NewTunerManager(&tuner.TunerManagerConfig{
			TunersConfig: config.TunersConfig{
				{
					Name:    "Tuner1",
					Types:   []string{"GR"},
					Command: "tuner <type> <channel> <freq> <polarity> <extra-args> <missing>",
				},
			},
		}),
	})
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	if err := session.RawStream(context.Background(), io.Discard); err != nil {
		t.Fatal(err)
	}

	if got, want := factory.count("tuner GR 27 12345 H --foo "), 1; got != want {
		t.Fatalf("replaced tuner process count = %d, want %d", got, want)
	}
}

func TestReplaceCommandTemplateMirakurunCompatibilityAliases(t *testing.T) {
	channel := &config.ChannelConfig{
		Type:        "BS",
		Channel:     "101",
		CommandVars: map[string]any{"satellite": "JCSAT3A", "space": uint8(1)},
	}

	got := replaceCommandTemplate("tuner <satellite> <satelite> <space> <missing>", channel)
	if want := "tuner JCSAT3A JCSAT3A 1 "; got != want {
		t.Fatalf("replaceCommandTemplate() = %q, want %q", got, want)
	}

	got = replaceCommandTemplate("tuner <space> <satelite>", &config.ChannelConfig{CommandVars: map[string]any{}})
	if want := "tuner 0 "; got != want {
		t.Fatalf("replaceCommandTemplate() default = %q, want %q", got, want)
	}
}

func TestRawStreamWithDecoder(t *testing.T) {
	factory := &fakeProcessFactory{}
	manager := testManager(factory, "decoder")
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	var out bytes.Buffer
	if err := session.RawStream(context.Background(), &out); err != nil {
		t.Fatal(err)
	}

	if got, want := out.String(), "decoded:ts2"; got != want {
		t.Fatalf("decoded stream = %q, want %q", got, want)
	}
	if got := factory.count("decoder"); got != 1 {
		t.Fatalf("decoder process count = %d, want 1", got)
	}
}

func TestConcurrentRawStreamsStartOneTuner(t *testing.T) {
	factory := &fakeProcessFactory{}
	manager := testManager(factory, "")
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

	if got := factory.count("tuner"); got != 1 {
		t.Fatalf("tuner process count = %d, want 1", got)
	}
	if first.String() == "" || second.String() == "" {
		t.Fatalf("both subscribers should receive data: first=%q second=%q", first.String(), second.String())
	}
}

func TestServiceStreamAndScanAttachToSharedSession(t *testing.T) {
	factory := &fakeProcessFactory{}
	manager := testManager(factory, "")
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

func TestScanServicesReportsMissingMirakcArib(t *testing.T) {
	factory := &fakeProcessFactory{ensureErr: ErrCommandNotFound}
	manager := testManager(factory, "")
	session, err := manager.GetOrCreate(context.Background(), "GR", "27")
	if err != nil {
		t.Fatal(err)
	}

	err = session.ScanServices(context.Background(), io.Discard)
	if !errors.Is(err, ErrCommandNotFound) {
		t.Fatalf("ScanServices error = %v, want ErrCommandNotFound", err)
	}
	if !strings.Contains(err.Error(), "mirakc-arib is required for service scanning") {
		t.Fatalf("ScanServices error = %q, want scanning context", err.Error())
	}
}
