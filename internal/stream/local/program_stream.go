package local

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/ts"
)

func (s *Session) programStream(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	gate := newProgramEventGate(p.NetworkID, p.ServiceID, p.EventID, programEventTimeout(p.StartAt, p.Duration), cancel)
	observerAttached := make(chan struct{})
	observeDone := make(chan error, 1)
	go func() {
		observeDone <- s.rawEngine.ObserveSectionsPassive(ctx, func(section ts.Section) bool {
			return ts.IsEITPF(section.TableID())
		}, func(section ts.Section) error {
			eit, err := ts.ParseEIT(section)
			if err == nil {
				gate.observe(eit)
			}
			return nil
		}, observerAttached)
	}()
	select {
	case <-observerAttached:
	case err := <-observeDone:
		return expectedNil(err)
	case <-ctx.Done():
		return expectedNil(ctx.Err())
	}

	r, w := io.Pipe()
	sourceDone := make(chan error, 1)
	go func() {
		sourceDone <- s.attachEngine(ctx, decode, p.ServiceID, true, w)
		_ = w.Close()
	}()
	err := runProgramGate(r, dst, gate)
	_ = r.Close()
	cancel()
	sourceErr := <-sourceDone
	observeErr := <-observeDone
	return errors.Join(expectedNil(err), expectedNil(sourceErr), expectedNil(observeErr))
}

func runProgramGate(src io.Reader, dst io.Writer, gate *programEventGate) error {
	packet := make([]byte, ts.PacketSize)
	var result error
	for {
		_, err := io.ReadFull(src, packet)
		if err != nil {
			result = expectedNil(err)
			break
		}
		if gate.isReady() {
			n, err := dst.Write(packet)
			if err == nil && n != len(packet) {
				err = io.ErrShortWrite
			}
			if err != nil {
				result = err
				break
			}
		}
	}
	return result
}

func programEventTimeout(startAt int64, duration int) time.Duration {
	timeout := time.Until(time.UnixMilli(startAt + int64(duration)))
	if duration == 1 {
		timeout += programEventMissingFallback
	}
	if timeout < 0 {
		return programEventMissingFallback
	}
	return timeout
}
