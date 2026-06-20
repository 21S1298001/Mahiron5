package filter

import (
	"context"
	"fmt"
	"io"
	"os/exec"
	"time"

	"github.com/21S1298001/Mahiron5/internal/processor"
	"github.com/21S1298001/Mahiron5/internal/util"
)

var (
	lookPath   = exec.LookPath
	newProcess = util.NewProcess
)

type ServiceFilter struct{}

func NewServiceFilter() *ServiceFilter {
	return &ServiceFilter{}
}

func (f *ServiceFilter) FilterService(ctx context.Context, serviceID uint16, src io.Reader, dst io.Writer) error {
	if _, err := lookPath("mirakc-arib"); err != nil {
		return fmt.Errorf("%w for service filtering: %w", processor.ErrMirakcAribRequired, err)
	}

	process := newProcess(fmt.Sprintf("mirakc-arib filter-service --sid %d", serviceID))
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
