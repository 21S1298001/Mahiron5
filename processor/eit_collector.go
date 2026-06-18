package processor

import (
	"context"
	"fmt"
	"io"
	"time"
)

type EITCollector struct{}

const (
	eitsCollectorCommand  = "mirakc-arib collect-eits"
	eitpfCollectorCommand = "mirakc-arib collect-eitpf --streaming"
)

func NewEITCollector() *EITCollector {
	return &EITCollector{}
}

func (c *EITCollector) CollectEITS(ctx context.Context, src io.Reader, dst io.Writer) error {
	return c.collect(ctx, eitsCollectorCommand, "EITS", src, dst)
}

func (c *EITCollector) CollectEITPF(ctx context.Context, src io.Reader, dst io.Writer) error {
	return c.collect(ctx, eitpfCollectorCommand, "EITPF", src, dst)
}

func (c *EITCollector) collect(ctx context.Context, command, name string, src io.Reader, dst io.Writer) error {
	if _, err := lookPath("mirakc-arib"); err != nil {
		return fmt.Errorf("%w for %s collection: %w", ErrMirakcAribRequired, name, err)
	}

	process := newProcess(command)
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
