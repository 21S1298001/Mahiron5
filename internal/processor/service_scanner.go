package processor

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/21S1298001/Mahiron5/internal/util"
)

var (
	lookPath   = exec.LookPath
	newProcess = util.NewProcess
)

type ServiceScanner struct{}

func NewServiceScanner() *ServiceScanner {
	return &ServiceScanner{}
}

func (s *ServiceScanner) ScanServices(ctx context.Context, src io.Reader, dst io.Writer) error {
	if _, err := lookPath("mirakc-arib"); err != nil {
		return fmt.Errorf("%w for service scanning: %w", ErrMirakcAribRequired, err)
	}

	process := newProcess("mirakc-arib scan-services")
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
