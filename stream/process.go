package stream

import (
	"context"
	"fmt"
	"io"
	"os/exec"

	"github.com/21S1298001/Mahiron5/util"
)

type Process interface {
	Stdin(io.Reader)
	Stdout(io.Writer)
	StdoutPipe() (io.ReadCloser, error)
	Start() error
	Wait() error
	Stop(context.Context) error
}

type ProcessFactory interface {
	EnsureCommand(name string) error
	NewProcess(command string) Process
}

type RealProcessFactory struct{}

func (RealProcessFactory) EnsureCommand(name string) error {
	if _, err := exec.LookPath(name); err != nil {
		return fmt.Errorf("%w: %s", ErrCommandNotFound, name)
	}
	return nil
}

func (RealProcessFactory) NewProcess(command string) Process {
	return util.NewProcess(command)
}

var ErrCommandNotFound = exec.ErrNotFound
