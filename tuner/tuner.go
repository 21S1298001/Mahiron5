package tuner

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"

	"github.com/21S1298001/Mahiron5/util/dynamicmultiwriter"
)

type Tuner struct {
	name      string
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
	defer t.writer.Detach(writer)

	if !t.streaming {
		slog.Info("request to start stream", "name", t.name, "stream", name)
		go func() {
			if err := t.spawn(); err != nil && !errors.Is(err, context.Canceled) {
				slog.Error("failed to spawn stream", "name", t.name, "stream", name, "err", err)
			}
		}()
	}

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			slog.Info("tuner detach stream", "name", t.name, "stream", name)
			return
		case <-ticker.C:
			if t.streaming {
				break
			}
			slog.Info("tuner stream closed", "name", t.name, "stream", name)
			return
		}
	}
}

func (t *Tuner) Shutdown(ctx context.Context) {
	t.writer.Close()
}

func (t *Tuner) spawn() error {
	t.streaming = true

	resp, err := http.Get("http://v6.haruka.dns.ggrel.net:40772/api/services/3273601024/stream")
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return errors.New("failed to get stream")
	}

	defer resp.Body.Close()
	defer t.writer.Close()

	slog.Info("tuner stream started", "name", t.name)
	_, err = io.Copy(t.writer, resp.Body)
	if err == nil {
		slog.Info("tuner stream ended", "name", t.name)
		t.streaming = false
		return nil
	}

	if errors.Is(err, io.ErrClosedPipe) {
		slog.Info("tuner stream closed", "name", t.name)
		t.streaming = false
		return nil
	}

	slog.Error("tuner stream closed unexpectedly", "name", t.name, "err", err)
	t.streaming = false
	return err
}
