package demuxer

import (
	"context"
	"errors"
	"io"
	"slices"
	"time"

	"github.com/21S1298001/Mahiron5/util/dynamicmultiwriter"
	"golang.org/x/sync/errgroup"
)

type demuxerType string

const (
	demuxerTypeEventParser    demuxerType = "event-parser"
	demuxerTypeLogoFetcher    demuxerType = "logo-fetcher"
	demuxerTypeProgramFilter  demuxerType = "program-filter"
	demuxerTypeRawStream      demuxerType = "raw-stream"
	demuxerTypeServiceFilter  demuxerType = "service-filter"
	demuxerTypeServiceScanner demuxerType = "service-scanner"
)

type demuxerPlugin interface {
	Start(src io.Reader) error
	Shutdown(ctx context.Context) error
	Count() int
	Attach(any) error
	Detach(any) error
}

type stream struct {
	EventId   uint16
	Plugin    demuxerPlugin
	ServiceId uint16
	Type      demuxerType
	Writer    io.Writer
}

type TSDemuxer struct {
	branch  *dynamicmultiwriter.DynamicMultiWriter
	cancel  chan<- struct{}
	decoder string
	err     <-chan error
	src     io.Reader
	streams []*stream
}

func NewTSDemuxer(src io.Reader, decoder string) *TSDemuxer {
	return &TSDemuxer{
		branch:  dynamicmultiwriter.New(),
		decoder: decoder,
		src:     src,
		streams: []*stream{},
	}
}

func (d *TSDemuxer) Start() error {
	if d.cancel != nil {
		return errors.New("Demuxer is already running")
	}

	cancelCh := make(chan struct{})
	errCh := make(chan error)

	go func() {
		defer close(cancelCh)
		defer close(errCh)

		_, err := io.Copy(d.branch, readerProxy(func(p []byte) (int, error) {
			select {
			case <-cancelCh:
				return 0, nil
			default:
				return d.src.Read(p)
			}
		}))
		errCh <- err
	}()

	d.cancel = cancelCh
	d.err = errCh

	return nil
}

func (d *TSDemuxer) Stop(ctx context.Context) error {
	defer d.branch.Close()

	eg := errgroup.Group{}
	for _, s := range d.streams {
		eg.Go(func() error {
			return s.Plugin.Shutdown(ctx)
		})
	}
	eg.Go(func() error {
		d.cancel <- struct{}{}
		select {
		case err := <-d.err:
			return err
		case <-ctx.Done():
			return errors.New("failed to stop demuxer")
		}
	})
	if err := eg.Wait(); err != nil {
		return err
	}

	return nil
}

func (d *TSDemuxer) RawStream(ctx context.Context, dst io.Writer) error {
	i := slices.IndexFunc(d.streams, func(s *stream) bool {
		return s.Type == demuxerTypeRawStream
	})

	var s *stream
	if i >= 0 {
		s = d.streams[i]
	} else {
		r, w := io.Pipe()
		s = &stream{
			EventId: 0,
			Plugin:  NewRawStream(),
			Type:    demuxerTypeRawStream,
			Writer:  w,
		}
		d.streams = append(d.streams, s)

		if err := s.Plugin.Start(r); err != nil {
			return err
		}

		d.branch.Attach(w)
	}

	if err := s.Plugin.Attach(dst); err != nil {
		return err
	}

	<-ctx.Done()

	if err := s.Plugin.Detach(dst); err != nil {
		return err
	}

	if s.Plugin.Count() > 0 {
		return nil
	}

	d.branch.Detach(s.Writer)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Plugin.Shutdown(ctx); err != nil {
		return err
	}

	return nil
}

func (d *TSDemuxer) ServiceFilter(ctx context.Context, serviceId uint16, dst io.Writer) error {
	i := slices.IndexFunc(d.streams, func(s *stream) bool {
		return s.Type == demuxerTypeServiceFilter && s.ServiceId == serviceId
	})

	var s *stream
	if i >= 0 {
		s = d.streams[i]
	} else {
		r, w := io.Pipe()
		s = &stream{
			EventId:   0,
			Plugin:    NewServiceFilter(serviceId),
			ServiceId: serviceId,
			Type:      demuxerTypeServiceFilter,
			Writer:    w,
		}
		d.streams = append(d.streams, s)

		if err := s.Plugin.Start(r); err != nil {
			return err
		}

		d.branch.Attach(w)
	}

	if err := s.Plugin.Attach(dst); err != nil {
		return err
	}

	<-ctx.Done()

	if err := s.Plugin.Detach(dst); err != nil {
		return err
	}

	if s.Plugin.Count() > 0 {
		return nil
	}

	d.branch.Detach(s.Writer)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.Plugin.Shutdown(ctx); err != nil {
		return err
	}

	return nil
}

func (d *TSDemuxer) ServiceScanner(ctx context.Context, callback ServiceScannerHandler) error {
	return nil
}
