package epg

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/21S1298001/Mahiron5/internal/util"
)

var ErrMirakcAribRequired = errors.New("mirakc-arib is required")

type Collector interface {
	CollectEITS(context.Context, io.Reader, io.Writer) error
	CollectEITPF(context.Context, io.Reader, io.Writer) error
}

type MirakcAribCollector struct{}

const (
	eitsCollectorCommand  = "mirakc-arib collect-eits"
	eitpfCollectorCommand = "mirakc-arib collect-eitpf --streaming"
)

var lookPath = exec.LookPath

func NewMirakcAribCollector() *MirakcAribCollector {
	return &MirakcAribCollector{}
}

func (c *MirakcAribCollector) CollectEITS(ctx context.Context, src io.Reader, dst io.Writer) error {
	return c.collect(ctx, eitsCollectorCommand, "EITS", src, dst)
}

func (c *MirakcAribCollector) CollectEITPF(ctx context.Context, src io.Reader, dst io.Writer) error {
	return c.collect(ctx, eitpfCollectorCommand, "EITPF", src, dst)
}

func (c *MirakcAribCollector) collect(ctx context.Context, command, name string, src io.Reader, dst io.Writer) error {
	if _, err := lookPath("mirakc-arib"); err != nil {
		return fmt.Errorf("%w for %s collection: %w", ErrMirakcAribRequired, name, err)
	}

	process := util.NewProcess(command)
	process.Stdin(src)
	process.Stdout(dst)
	if err := process.Start(); err != nil {
		return err
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- process.Wait()
	}()

	select {
	case err := <-waitCh:
		return err
	case <-ctx.Done():
		stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return process.Stop(stopCtx)
	}
}
