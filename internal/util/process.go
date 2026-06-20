package util

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

type Process struct {
	cmd      *exec.Cmd
	command  string
	done     chan struct{}
	err      error
	mu       sync.Mutex
	stopping bool
	wait     sync.Once
}

func NewProcess(command string) *Process {
	cmd := exec.Command("sh", "-c", command)
	cmd.SysProcAttr = &syscall.SysProcAttr{
		Setpgid: true,
	}

	return &Process{
		cmd:     cmd,
		command: command,
		done:    make(chan struct{}),
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
	p.wait.Do(func() {
		err := p.wrapError(p.cmd.Wait())
		p.mu.Lock()
		if p.stopping {
			err = ignoreTerminated(err)
		}
		p.err = err
		p.mu.Unlock()
		close(p.done)
	})
	<-p.done
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *Process) Stop(ctx context.Context) error {
	if p.cmd.Process == nil {
		return nil
	}

	select {
	case <-p.done:
		return p.Wait()
	default:
	}

	p.mu.Lock()
	p.stopping = true
	p.mu.Unlock()

	if err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGTERM); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
		return err
	}

	waitCh := make(chan error, 1)
	go func() {
		waitCh <- p.Wait()
	}()

	select {
	case err := <-waitCh:
		return ignoreTerminated(err)
	case <-ctx.Done():
		if err := syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL); err != nil && !errors.Is(err, os.ErrProcessDone) && !errors.Is(err, syscall.ESRCH) {
			return err
		}
		return ignoreTerminated(<-waitCh)
	}
}

func (p *Process) RunWithContext(ctx context.Context, stopTimeout time.Duration) error {
	if err := p.Start(); err != nil {
		return err
	}

	ch := make(chan error, 1)
	go func() {
		ch <- p.Wait()
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

func ignoreTerminated(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || strings.Contains(err.Error(), "signal: terminated") || strings.Contains(err.Error(), "signal: killed") {
		return nil
	}
	return err
}

func (p *Process) wrapError(err error) error {
	if err == nil {
		return nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) && exitErr.ExitCode() == 127 {
		return fmt.Errorf("command %q failed with exit status 127 (command not found in shell PATH): %w", p.command, err)
	}
	return fmt.Errorf("command %q failed: %w", p.command, err)
}
