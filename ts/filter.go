package ts

import (
	"context"
	"errors"
	"io"
)

// ServiceFilter reads a raw TS stream and writes only packets belonging to the given service.
type ServiceFilter struct {
	serviceID uint16
}

// NewServiceFilter creates a filter for the given service_id.
func NewServiceFilter(serviceID uint16) *ServiceFilter {
	return &ServiceFilter{serviceID: serviceID}
}

// Filter copies only packets required for the service from src to dst.
// Required PIDs are PAT, PMT, PCR, and elementary stream PIDs referenced by the service.
func (f *ServiceFilter) Filter(ctx context.Context, src io.Reader, dst io.Writer) error {
	// TODO: implement service filtering.
	return errors.New("ts: Filter not implemented")
}
