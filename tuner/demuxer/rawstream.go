package demuxer

import (
	"context"
	"errors"
	"io"

	"github.com/21S1298001/Mahiron5/util"
)

type readerProxy func(p []byte) (n int, err error)

func (rf readerProxy) Read(p []byte) (n int, err error) { return rf(p) }

func Copy(ctx context.Context, dst io.Writer, src io.Reader) error {
	_, err := io.Copy(dst, readerProxy(func(p []byte) (int, error) {
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		default:
			return src.Read(p)
		}
	}))
	return err
}

var _ demuxerPlugin = (*rawStream)(nil)

type rawStream struct {
	cancel chan<- struct{}
	err    <-chan error
	writer *util.DynamicMultiWriter
}

func NewRawStream() *rawStream {
	return &rawStream{
		writer: util.NewDynamicMultiWriter(),
	}
}

func (r *rawStream) Start(src io.Reader) error {
	if r.cancel != nil {
		return errors.New("RawStream is already running")
	}

	if r.writer == nil {
		return errors.New("RawStream is not initialized")
	}

	cancelCh := make(chan struct{})
	errCh := make(chan error)

	go func() {
		defer close(cancelCh)
		defer close(errCh)

		_, err := io.Copy(r.writer, readerProxy(func(p []byte) (int, error) {
			select {
			case <-cancelCh:
				return 0, nil
			default:
				return src.Read(p)
			}
		}))
		errCh <- err
	}()

	r.cancel = cancelCh
	r.err = errCh

	return nil
}

func (r *rawStream) Shutdown(ctx context.Context) error {
	if r.cancel == nil {
		return nil
	}

	defer func() {
		r.cancel = nil
		r.err = nil
		r.writer = nil
	}()

	r.writer.Close()

	r.cancel <- struct{}{}
	select {
	case err := <-r.err:
		return err
	case <-ctx.Done():
		return errors.New("failed to stop raw stream")
	}
}

func (r *rawStream) Count() int {
	if r.writer == nil {
		return 0
	}
	return r.writer.Count()
}

func (r *rawStream) Attach(stream any) error {
	if r.writer == nil {
		return errors.New("RawStream is not running")
	}

	if _, ok := stream.(io.Writer); !ok || stream == nil {
		return errors.New("stream is not a io.Writer")
	}

	r.writer.Attach(stream.(io.Writer))

	return nil
}

func (r *rawStream) Detach(stream any) error {
	if r.writer == nil {
		return errors.New("RawStream is not running")
	}

	if _, ok := stream.(io.Writer); !ok || stream == nil {
		return errors.New("stream is not a io.Writer")
	}

	r.writer.Detach(stream.(io.Writer))

	return nil
}
