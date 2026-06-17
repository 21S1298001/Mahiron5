package processor

import (
	"context"
	"fmt"
	"io"
	"time"
)

type EITCollector struct{}

func NewEITCollector() *EITCollector {
	return &EITCollector{}
}

func (c *EITCollector) CollectEITS(ctx context.Context, src io.Reader, dst io.Writer) error {
	return c.collect(ctx, "mirakc-arib collect-eits", "EITS", src, dst)
}

func (c *EITCollector) CollectEITPF(ctx context.Context, src io.Reader, dst io.Writer) error {
	return c.collect(ctx, "mirakc-arib collect-eitpf --streaming", "EITPF", src, dst)
}

func (c *EITCollector) collect(ctx context.Context, command, name string, src io.Reader, dst io.Writer) error {
	if _, err := lookPath("mirakc-arib"); err != nil {
		return fmt.Errorf("mirakc-arib is required for %s collection: %w", name, err)
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
