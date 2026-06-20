package servicescan

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/21S1298001/Mahiron5/internal/util"
)

// ErrMirakcAribRequired is returned by service scan adapters that still depend
// on the external `mirakc-arib` CLI.
var ErrMirakcAribRequired = errors.New("mirakc-arib is required")

const serviceScannerCommand = "mirakc-arib scan-services"

var lookPath = exec.LookPath

type MirakcAribScanner struct{}

func NewMirakcAribScanner() *MirakcAribScanner {
	return &MirakcAribScanner{}
}

func (s *MirakcAribScanner) ScanServices(ctx context.Context, src io.Reader, dst io.Writer) error {
	if _, err := lookPath("mirakc-arib"); err != nil {
		return fmt.Errorf("%w for service scanning: %w", ErrMirakcAribRequired, err)
	}

	process := util.NewProcess(serviceScannerCommand)
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
