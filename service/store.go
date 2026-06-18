package service

import "context"

type Store interface {
	List(ctx context.Context) ([]*Service, error)
	GetByID(ctx context.Context, id string) (*Service, error)
	GetByChannel(ctx context.Context, channelType, channelId string) ([]*Service, error)
	ReplaceChannelServices(ctx context.Context, channelType, channelId string, services []*Service) error
	PruneChannels(ctx context.Context, active []ChannelKey) error
	SetEPGAttempt(ctx context.Context, networkID, serviceID uint16, attemptedAt int64, lastError string) error
	SetEPGSuccess(ctx context.Context, networkID, serviceID uint16, succeededAt int64) error
}

type ChannelKey struct {
	Type string
	ID   string
}
