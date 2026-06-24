package stream

import (
	"context"

	"github.com/21S1298001/mahiron/ts"
)

type ServiceScannerAdapter struct {
	manager *StreamManager
}

func NewServiceScannerAdapter(manager *StreamManager) *ServiceScannerAdapter {
	return &ServiceScannerAdapter{manager: manager}
}

func (a *ServiceScannerAdapter) ScanServices(ctx context.Context, channelType, channelID string, wait bool) ([]ts.ServiceInfo, error) {
	var (
		session Session
		err     error
	)
	if wait {
		session, err = a.manager.GetOrCreateWait(ctx, channelType, channelID)
	} else {
		session, err = a.manager.GetOrCreate(ctx, channelType, channelID)
	}
	if err != nil {
		return nil, err
	}
	return session.ScanServices(ctx)
}
