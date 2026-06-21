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
	return c.collect(ctx, src, dst, IsEITS)
}

// CollectEITPF collects EIT present/following sections and writes JSON lines to dst.
func (c *EITCollector) CollectEITPF(ctx context.Context, src io.Reader, dst io.Writer) error {
	return c.collect(ctx, src, dst, IsEITPF)
}

func (c *EITCollector) collect(ctx context.Context, src io.Reader, dst io.Writer, accept func(byte) bool) error {
	reader := NewPacketReader(src)
	assembler := NewSectionAssembler(PIDEIT)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		packet, err := reader.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		if packet.PID() != PIDEIT || packet.TransportErrorIndicator() || packet.IsNull() || !packet.ValidPayloadOffset() {
			continue
		}
		sections, err := assembler.FeedAll(packet)
		if err != nil {
			return err
		}
		for _, section := range sections {
			if !accept(section.TableID()) {
				continue
			}
			eit, err := ParseEIT(section)
			if err != nil {
				continue
			}
			line, err := marshalEITJSONLine(eit)
			if err != nil {
				return err
			}
			if _, err := dst.Write(line); err != nil {
				return err
			}
		}
	}
}
