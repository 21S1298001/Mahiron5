package tuner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"

	"github.com/21S1298001/Mahiron5/util/dynamicmultiwriter"
)

type TunerCommand interface {
	Name() string
}

type TunerCommandStart struct{}

func (c TunerCommandStart) Name() string {
	return "start"
}

type TunerCommandStop struct{}

func (c TunerCommandStop) Name() string {
	return "stop"
}

type Tuner struct {
	name      string
	ctx       context.Context
	cancel    context.CancelFunc
	streaming bool
	writer    *dynamicmultiwriter.DynamicMultiWriter
}

func NewTuner(name string) *Tuner {
	return &Tuner{
		name:   name,
		writer: dynamicmultiwriter.New([]io.Writer{}),
	}
}

func (t *Tuner) StartStream(ctx context.Context, name string, writer io.Writer) {
	slog.Info("tuner attach stream", "name", t.name, "stream", name)

	t.writer.Attach(writer)

	if !t.streaming {
		t.ctx, t.cancel = context.WithCancel(context.Background())
		slog.Info("request to start stream", "name", t.name, "stream", name)
		go func() {
			if err := t.spawn(t.ctx); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("failed to spawn stream", "name", t.name, "stream", name, "err", err)
			}
		}()
	}

	select {
	case <-ctx.Done():
		slog.Info("tuner detach stream", "name", t.name, "stream", name)
		t.writer.Detach(writer)
		if t.writer.Count() == 0 {
			slog.Info("request to stop stream", "name", t.name, "stream", name)
			t.cancel()
		}
	case <-t.ctx.Done():
		slog.Info("tuner closed", "name", t.name, "stream", name)
		t.writer.Detach(writer)
	}
}

func (t *Tuner) Shutdown(ctx context.Context) {
	if t.cancel != nil {
		slog.Info("request to stop stream", "name", t.name)
		t.cancel()
	}

	t.writer.Close()
}

func (t *Tuner) spawn(ctx context.Context) error {
	resp, err := http.Get("http://v6.haruka.dns.ggrel.net:40772/api/services/3273601024/stream")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("failed to get stream")
	}

	defer resp.Body.Close()
	defer t.writer.Close()

	t.streaming = true

	slog.Info("tuner stream started", "name", t.name)
	_, err = io.Copy(t.writer, resp.Body)
	if err != nil {
		t.streaming = false
		return err
	}

	<-ctx.Done()
	slog.Info("tuner stream stopped", "name", t.name)
	t.streaming = false
	return ctx.Err()
}
