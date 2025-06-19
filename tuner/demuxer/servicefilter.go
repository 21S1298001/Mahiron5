package demuxer

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/21S1298001/Mahiron5/util"
)

var _ demuxerPlugin = (*serviceFilter)(nil)

type serviceFilter struct {
	ServiceId uint16
	process   *util.Process
	writer    *util.DynamicMultiWriter
}

func NewServiceFilter(serviceId uint16) *serviceFilter {
	return &serviceFilter{
		ServiceId: serviceId,
		process:   nil,
		writer:    util.NewDynamicMultiWriter(),
	}
}

func (s *serviceFilter) Start(src io.Reader) error {
	if s.process != nil {
		return errors.New("ServiceFilter is already running")
	}

	if s.writer == nil {
		return errors.New("ServiceFilter is not initialized")
	}

	s.process = util.NewProcess(fmt.Sprintf("mirakc-arib filter-service --sid %d", s.ServiceId))

	s.process.Stdin(src)
	s.process.Stdout(s.writer)

	if err := s.process.Start(); err != nil {
		return err
	}
	return nil
}

func (s *serviceFilter) Shutdown(ctx context.Context) error {
	if s.process == nil {
		return nil
	}

	defer func() {
		s.process = nil
		s.writer = nil
	}()

	s.writer.Close()

	if err := s.process.Stop(ctx); err != nil {
		return err
	}

	return nil
}

func (s *serviceFilter) Count() int {
	if s.writer == nil {
		return 0
	}
	return s.writer.Count()
}

func (s *serviceFilter) Attach(stream any) error {
	if s.writer == nil {
		return errors.New("ServiceFilter is not running")
	}

	if _, ok := stream.(io.Writer); !ok || stream == nil {
		return errors.New("stream is not a io.Writer")
	}

	s.writer.Attach(stream.(io.Writer))

	return nil
}

func (s *serviceFilter) Detach(stream any) error {
	if s.writer == nil {
		return errors.New("ServiceFilter is not running")
	}

	if _, ok := stream.(io.Writer); !ok || stream == nil {
		return errors.New("stream is not a io.Writer")
	}

	s.writer.Detach(stream.(io.Writer))

	return nil
}
