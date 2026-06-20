package processor

import (
	"context"
	"io"
)

type Processor interface {
	Run(context.Context, io.Reader, io.Writer) error
}

type ProcessorFunc func(context.Context, io.Reader, io.Writer) error

func (f ProcessorFunc) Run(ctx context.Context, src io.Reader, dst io.Writer) error {
	return f(ctx, src, dst)
}
