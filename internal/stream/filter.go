package stream

import (
	"context"
	"io"

	"github.com/21S1298001/Mahiron5/ts"
)

type NativeServiceFilter struct{}

func (NativeServiceFilter) FilterService(ctx context.Context, serviceID uint16, src io.Reader, dst io.Writer) error {
	return ts.NewServiceFilter(serviceID).Filter(ctx, src, dst)
}
