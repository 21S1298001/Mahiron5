package demuxer

import (
	"context"
	"encoding/json"
	"errors"
	"io"

	"slices"

	"github.com/21S1298001/Mahiron5/util"
	"golang.org/x/sync/errgroup"
)

type ScannedService struct {
	NetworkId          uint16 `json:"nid"`
	TransportStreamId  uint16 `json:"tsid"`
	ServiceId          uint16 `json:"sid"`
	Name               string `json:"name"`
	Type               uint8  `json:"type"`
	LogoId             uint16 `json:"logo_id"`
	RemoteControlKeyId uint16 `json:"remoteControlKeyId"`
}

type ServiceScannerHandler func([]*ScannedService)

var _ demuxerPlugin = (*serviceScanner)(nil)

type serviceScanner struct {
	handlers []ServiceScannerHandler
	process  *util.Process
	services []*ScannedService
}

func NewServiceScanner() *serviceScanner {
	return &serviceScanner{
		handlers: []ServiceScannerHandler{},
		process:  nil,
	}
}

func (s *serviceScanner) Start(src io.Reader) error {
	if s.process != nil {
		return errors.New("ServiceScanner is already running")
	}

	if s.handlers == nil {
		return errors.New("ServiceScanner is not initialized")
	}

	s.process = util.NewProcess("mirakc-arib scan-services")

	r, w := io.Pipe()

	s.process.Stdin(src)
	s.process.Stdout(w)

	if err := s.process.Start(); err != nil {
		return err
	}

	go func() {
		if err := json.NewDecoder(r).Decode(&s.services); err != nil {
			return
		}

		if s.handlers == nil {
			return
		}

		eg := errgroup.Group{}
		for _, handler := range s.handlers {
			eg.Go(func() error {
				handler(s.services)
				return nil
			})
		}
		if err := eg.Wait(); err != nil {
			return
		}
	}()

	return nil
}

func (s *serviceScanner) Shutdown(ctx context.Context) error {
	if s.process == nil {
		return nil
	}

	defer func() {
		s.handlers = nil
		s.process = nil
		s.services = nil
	}()

	if err := s.process.Stop(ctx); err != nil {
		return err
	}

	return nil
}

func (s *serviceScanner) Count() int {
	return len(s.services)
}

func (s *serviceScanner) Attach(handler any) error {
	if s.process == nil {
		return errors.New("ServiceScanner is not running")
	}

	handlerFunc, ok := handler.(ServiceScannerHandler)
	if !ok {
		return errors.New("handler is not a ServiceScannerHandler")
	}

	s.handlers = append(s.handlers, handlerFunc)

	return nil
}

func (s *serviceScanner) Detach(handler any) error {
	if s.process == nil {
		return errors.New("ServiceScanner is not running")
	}

	handlerFunc, ok := handler.(ServiceScannerHandler)
	if !ok {
		return errors.New("handler is not a ServiceScannerHandler")
	}

	for i, h := range s.handlers {
		if &h == &handlerFunc {
			s.handlers = slices.Delete(s.handlers, i, i+1)
			break
		}
	}

	return nil
}
