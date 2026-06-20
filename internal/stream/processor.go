package stream

import (
	"context"
	"io"

	"github.com/21S1298001/Mahiron5/internal/processor"
)

type Processor = processor.Processor

type errorProcessor struct {
	err error
}

func (p errorProcessor) Run(context.Context, io.Reader, io.Writer) error {
	return p.err
}

type descramblerProcessor struct {
	descrambler Descrambler
}

func (p descramblerProcessor) Run(ctx context.Context, src io.Reader, dst io.Writer) error {
	return p.descrambler.Descramble(ctx, src, dst)
}

type serviceFilterProcessor struct {
	filter    ServiceFilter
	serviceID uint16
}

func (p serviceFilterProcessor) Run(ctx context.Context, src io.Reader, dst io.Writer) error {
	return p.filter.FilterService(ctx, p.serviceID, src, dst)
}
