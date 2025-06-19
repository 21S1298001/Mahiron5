package tuner

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/21S1298001/Mahiron5/util"
)

type TunerEntity interface {
	Command() string
	Pid() int
	Stream(ctx context.Context, dst io.Writer) error
	Shutdown(ctx context.Context) error
}

var _ TunerEntity = (*CommandTunerEntity)(nil)

type CommandTunerEntity struct {
	command string
	process *util.Process
}

func NewCommandTunerEntity(command string) *CommandTunerEntity {
	return &CommandTunerEntity{
		command: command,
	}
}

func (c *CommandTunerEntity) Command() string {
	return c.command
}

func (c *CommandTunerEntity) Pid() int {
	if c.process != nil {
		return c.process.Pid()
	}
	return 0
}

func (c *CommandTunerEntity) Stream(ctx context.Context, dst io.Writer) error {
	if c.process != nil {
		return errors.New("Tuner is already running")
	}

	defer func() {
		c.process = nil
	}()

	c.process = util.NewProcess(c.command)
	c.process.Stdout(dst)

	if err := c.process.RunWithContext(ctx, 5*time.Second); err != nil {
		return err
	}

	return nil
}

func (c *CommandTunerEntity) Shutdown(ctx context.Context) error {
	if c.process == nil {
		return nil
	}
	if err := c.process.Stop(ctx); err != nil {
		return err
	}
	return nil
}
