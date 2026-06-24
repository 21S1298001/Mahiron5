package stream

import (
	"context"
	"io"

	"github.com/21S1298001/mahiron/internal/config"
	"github.com/21S1298001/mahiron/internal/program"
	"github.com/21S1298001/mahiron/ts"
)

type RemoteSessionConfig struct {
	Client       *RemoteClient
	Channel      *config.ChannelConfig
	RouteChannel *config.ChannelConfig
}

type RemoteSession struct {
	channel      *config.ChannelConfig
	client       *RemoteClient
	routeChannel *config.ChannelConfig
}

func NewRemoteSession(config RemoteSessionConfig) *RemoteSession {
	return &RemoteSession{
		channel:      config.Channel,
		client:       config.Client,
		routeChannel: config.RouteChannel,
	}
}

func (s *RemoteSession) ChannelStream(ctx context.Context, decode bool, dst io.Writer) error {
	return s.client.ChannelStream(ctx, s.routeChannel.Type, s.routeChannel.Channel, decode, dst)
}

func (s *RemoteSession) ServiceStream(ctx context.Context, serviceID uint16, decode bool, dst io.Writer) error {
	return s.client.ServiceStream(ctx, s.routeChannel.Type, s.routeChannel.Channel, serviceID, decode, dst)
}

func (s *RemoteSession) ProgramStream(ctx context.Context, p *program.Program, decode bool, dst io.Writer) error {
	return s.client.ProgramStream(ctx, p.ID, decode, dst)
}

func (s *RemoteSession) ScanServices(ctx context.Context) ([]ts.ServiceInfo, error) {
	return s.client.ScanServices(ctx, s.routeChannel.Type, s.routeChannel.Channel)
}

func (s *RemoteSession) ListServicePrograms(ctx context.Context, networkID, serviceID uint16) ([]*program.Program, error) {
	return s.client.ListServicePrograms(ctx, networkID, serviceID)
}

func (s *RemoteSession) CollectEIT(context.Context, func(*ts.EIT) error) error {
	return ErrEITObservationUnsupported
}

func (s *RemoteSession) ObserveLogos(ctx context.Context, observe func(*ts.LogoImage) error) error {
	services, err := s.client.ListChannelServices(ctx, s.routeChannel.Type, s.routeChannel.Channel)
	if err != nil {
		return err
	}
	for _, svc := range services {
		if !remoteServiceHasLogo(svc) {
			continue
		}
		data, err := s.client.GetLogoImage(ctx, int64(svc.NetworkID)*100000+int64(svc.ServiceID))
		if err != nil {
			return err
		}
		image := &ts.LogoImage{
			OriginalNetworkID: svc.NetworkID,
			LogoID:            uint16(*svc.LogoID),
			LogoVersion:       *remoteLogoVersion(),
			DownloadDataID:    *remoteLogoDownloadDataID(svc),
			LogoType:          5,
			Data:              data,
		}
		if err := observe(image); err != nil {
			return err
		}
	}
	return nil
}

func (s *RemoteSession) Stop(context.Context) error {
	return nil
}
