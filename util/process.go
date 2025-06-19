package util

import (
	"context"
	"errors"
	"io"
	"os/exec"
	"syscall"
	"time"
)

type Process struct {
	cmd     *exec.Cmd
	command string
}

func NewProcess(command string) *Process {
	cmd := exec.Command("sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	return &Process{
		cmd:     cmd,
		command: command,
	}
}

func (p *Process) StdinPipe() (io.WriteCloser, error) {
	return p.cmd.StdinPipe()
}

func (p *Process) StdoutPipe() (io.ReadCloser, error) {
	return p.cmd.StdoutPipe()
}

func (p *Process) StderrPipe() (io.ReadCloser, error) {
	return p.cmd.StderrPipe()
}

func (p *Process) Stdin(stdin io.Reader) {
	p.cmd.Stdin = stdin
}

func (p *Process) Stdout(stdout io.Writer) {
	p.cmd.Stdout = stdout
}

func (p *Process) Stderr(stderr io.Writer) {
	p.cmd.Stderr = stderr
}

func (p *Process) Pid() int {
	if p.cmd.Process == nil {
		return 0
	}
	return p.cmd.Process.Pid
}

func (p *Process) Start() error {
	return p.cmd.Start()
}

func (p *Process) Run() error {
	return p.cmd.Run()
}

func (p *Process) Wait() error {
	return p.cmd.Wait()
}

func (p *Process) Stop(ctx context.Context) error {
	if err := p.cmd.Process.Signal(syscall.SIGTERM); err != nil {
		return err
	}

	if err := p.cmd.Wait(); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || err.Error() == "signal: terminated" {
			return nil
		}
		return err
	}

	<-ctx.Done()
	if err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL); err != nil {
		return err
	}
	return nil
}

func (p *Process) RunWithContext(ctx context.Context, stopTimeout time.Duration) error {
	ch := make(chan error)
	go func() {
		if err := p.cmd.Run(); err != nil {
			ch <- err
		}
		ch <- nil
	}()

	select {
	case err := <-ch:
		return err
	case <-ctx.Done():
		termCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
		defer cancel()
		return p.Stop(termCtx)
	}
}
