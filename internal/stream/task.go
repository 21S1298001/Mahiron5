package stream

import (
	"context"
	"io"
)

type StreamTaskRunner struct {
	source interface {
		Subscribe(context.Context, io.Writer) error
		Err() error
	}
}

func NewStreamTaskRunner(source interface {
	Subscribe(context.Context, io.Writer) error
	Err() error
}) StreamTaskRunner {
	return StreamTaskRunner{source: source}
}

func (r StreamTaskRunner) Run(ctx context.Context, dst io.Writer, task func(context.Context, io.Reader, io.Writer) error) error {
	return r.RunTask(ctx, func(ctx context.Context, src io.Reader) error {
		return task(ctx, src, dst)
	})
}

func (r StreamTaskRunner) RunTask(ctx context.Context, task func(context.Context, io.Reader) error) error {
	pr, pw := io.Pipe()
	taskCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	sourceCtx, stopSource := context.WithCancel(ctx)
	defer stopSource()

	taskDone := make(chan error, 1)
	go func() {
		taskDone <- task(taskCtx, pr)
	}()

	sourceDone := make(chan error, 1)
	go func() {
		sourceDone <- r.source.Subscribe(sourceCtx, pw)
		_ = pw.Close()
	}()

	select {
	case err := <-taskDone:
		_ = pw.Close()
		cancel()
		stopSource()
		<-sourceDone
		return err
	case <-ctx.Done():
		_ = pw.Close()
		cancel()
		stopSource()
		<-sourceDone
		return <-taskDone
	case err := <-sourceDone:
		_ = pw.Close()
		cancel()
		taskErr := <-taskDone
		if err != nil {
			return err
		}
		return taskErr
	}
}
