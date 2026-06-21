package filter

import (
	"context"
	"io"

	"github.com/21S1298001/Mahiron5/ts"
)

type ServiceFilter struct{}

func NewServiceFilter() *ServiceFilter {
	return &ServiceFilter{}
}

func (f *ServiceFilter) FilterService(ctx context.Context, serviceID uint16, src io.Reader, dst io.Writer) error {
	return ts.NewServiceFilter(serviceID).Filter(ctx, src, dst)
}
