package service

import "context"

type Store interface {
	List(ctx context.Context) ([]*Service, error)
	GetByID(ctx context.Context, id string) (*Service, error)
	GetByChannel(ctx context.Context, channelType, channelId string) ([]*Service, error)
	ReplaceChannelServices(ctx context.Context, channelType, channelId string, services []*Service) error
	PruneChannels(ctx context.Context, active []ChannelKey) error
}

type ChannelKey struct {
	Type string
	ID   string
}
