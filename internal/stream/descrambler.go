package stream

import (
	"context"
	"io"
	"time"

	"github.com/21S1298001/mahiron/internal/util"
)

type Descrambler interface {
	Descramble(context.Context, io.Reader, io.Writer) error
}

type DescramblerFactory func(string) Descrambler

type CommandDescrambler struct {
	command string
}

func NewCommandDescrambler(command string) Descrambler {
	if command == "" {
		return nil
	}
	return CommandDescrambler{command: command}
}

func (d CommandDescrambler) Descramble(ctx context.Context, src io.Reader, dst io.Writer) error {
	process := util.NewProcess(d.command)
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
