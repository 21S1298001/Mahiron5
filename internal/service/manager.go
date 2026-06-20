package service

import (
	"context"
	"strconv"

	"github.com/21S1298001/Mahiron5/internal/config"
)

type ServiceManager struct {
	store    Store
	channels config.ChannelsConfig
}

func NewServiceManager(store Store, channels config.ChannelsConfig) *ServiceManager {
	return &ServiceManager{
		store:    store,
		channels: channels,
	}
}

func (s *ServiceManager) CountServices(ctx context.Context) (int, error) {
	services, err := s.store.List(ctx)
	if err != nil {
		return 0, err
	}
	return len(services), nil
}

func (s *ServiceManager) GetServices(ctx context.Context) ([]*Service, error) {
	return s.store.List(ctx)
}

func (s *ServiceManager) SetEPGAttempt(ctx context.Context, networkID, serviceID uint16, attemptedAt int64, lastError string) error {
	return s.store.SetEPGAttempt(ctx, networkID, serviceID, attemptedAt, lastError)
}

func (s *ServiceManager) SetEPGSuccess(ctx context.Context, networkID, serviceID uint16, succeededAt int64) error {
	return s.store.SetEPGSuccess(ctx, networkID, serviceID, succeededAt)
}

func (s *ServiceManager) EPGSummary(ctx context.Context, staleAfter int64, now int64) (stale, failed int, lastSuccess *int64, err error) {
	services, err := s.store.List(ctx)
	if err != nil {
		return 0, 0, nil, err
	}
	for _, svc := range services {
		if svc.EPG.LastSuccessAt == nil || now-*svc.EPG.LastSuccessAt > staleAfter {
			stale++
		}
		if svc.EPG.LastError != "" {
			failed++
		}
		if svc.EPG.LastSuccessAt != nil && (lastSuccess == nil || *svc.EPG.LastSuccessAt > *lastSuccess) {
			v := *svc.EPG.LastSuccessAt
			lastSuccess = &v
		}
	}
	return
}

func (s *ServiceManager) ReconcileChannels(ctx context.Context) error {
	active := make([]ChannelKey, 0, len(s.channels))
	for _, channel := range s.channels {
		if !isDisabled(channel) {
			active = append(active, ChannelKey{Type: channel.Type, ID: channel.Channel})
		}
	}
	return s.store.PruneChannels(ctx, active)
}

func (s *ServiceManager) GetServiceById(ctx context.Context, id string) (*Service, error) {
	// Try exact string ID match first
	svc, err := s.store.GetByID(ctx, id)
	if err != nil {
		return nil, err
	}
	if svc != nil {
		return svc, nil
	}

	// Fall back to ItemId() match
	parsedId, parseErr := strconv.ParseInt(id, 10, 64)
	if parseErr != nil {
		return nil, nil
	}
	services, err := s.store.List(ctx)
	if err != nil {
		return nil, err
	}
	for _, service := range services {
		if service.ItemId() == parsedId {
			return service, nil
		}
	}
	return nil, nil
}

func (s *ServiceManager) GetChannels() config.ChannelsConfig {
	channels := make(config.ChannelsConfig, 0, len(s.channels))
	for _, channel := range s.channels {
		if isDisabled(channel) {
			continue
		}
		channels = append(channels, channel)
	}
	return channels
}

func (s *ServiceManager) GetChannel(channelType string, channelId string) *config.ChannelConfig {
	for i := range s.channels {
		if s.channels[i].Type == channelType && s.channels[i].Channel == channelId && !isDisabled(s.channels[i]) {
			channel := s.channels[i]
			return &channel
		}
	}
	return nil
}

func (s *ServiceManager) GetServicesByChannel(ctx context.Context, channelType string, channelId string) ([]*Service, error) {
	return s.store.GetByChannel(ctx, channelType, channelId)
}

func (s *ServiceManager) GetServiceByChannelAndId(ctx context.Context, channelType string, channelId string, id string) (*Service, error) {
	services, err := s.store.GetByChannel(ctx, channelType, channelId)
	if err != nil {
		return nil, err
	}
	parsedId, parseErr := strconv.ParseInt(id, 10, 64)
	for _, service := range services {
		if service.Id == id || (parseErr == nil && service.ItemId() == parsedId) {
			return service, nil
		}
	}
	return nil, nil
}

func isDisabled(channel config.ChannelConfig) bool {
	return channel.IsDisabled != nil && *channel.IsDisabled
}
