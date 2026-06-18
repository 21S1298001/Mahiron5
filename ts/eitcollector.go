package ts

import (
	"context"
	"errors"
	"io"
)

// EITCollector reads TS streams and outputs EIT sections as JSON lines.
type EITCollector struct{}

// NewEITCollector creates a new EITCollector.
func NewEITCollector() *EITCollector {
	return &EITCollector{}
}

// CollectEITS collects EIT schedule sections and writes JSON lines to dst.
func (c *EITCollector) CollectEITS(ctx context.Context, src io.Reader, dst io.Writer) error {
	// TODO: implement EITS collection.
	return errors.New("ts: CollectEITS not implemented")
}

// CollectEITPF collects EIT present/following sections and writes JSON lines to dst.
func (c *EITCollector) CollectEITPF(ctx context.Context, src io.Reader, dst io.Writer) error {
	// TODO: implement EITPF collection.
	return errors.New("ts: CollectEITPF not implemented")
}
