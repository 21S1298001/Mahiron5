package ts

import (
	"context"
	"errors"
	"io"
)

// ServiceInfo represents a scanned service, matching the JSON output of mirakc-arib scan-services.
type ServiceInfo struct {
	Nid                uint16 `json:"nid"`
	Tsid               uint16 `json:"tsid"`
	Sid                uint16 `json:"sid"`
	Name               string `json:"name"`
	Type               uint8  `json:"type"`
	LogoId             uint64 `json:"logoId"`
	RemoteControlKeyId uint8  `json:"remoteControlKeyId"`
}

// ServiceScanner reads a TS stream and outputs a list of services.
type ServiceScanner struct{}

// NewServiceScanner creates a new ServiceScanner.
func NewServiceScanner() *ServiceScanner {
	return &ServiceScanner{}
}

// Scan reads TS from src and writes a JSON array of ServiceInfo to dst.
func (s *ServiceScanner) Scan(ctx context.Context, src io.Reader, dst io.Writer) error {
	// TODO: implement service scanning.
	return errors.New("ts: Scan not implemented")
}
