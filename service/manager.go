package service

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/21S1298001/Mahiron5/config"
	"github.com/21S1298001/Mahiron5/stream"
)

type ServiceManager struct {
	channels config.ChannelsConfig
	services []*Service
}

type ServiceManagerConfig struct {
	Channels config.ChannelsConfig
}

func NewServiceManager(config *ServiceManagerConfig) *ServiceManager {
	return &ServiceManager{
		channels: config.Channels,
	}
}

func (s *ServiceManager) CountServices() int {
	return len(s.services)
}

func (s *ServiceManager) GetServices() []*Service {
	return s.services
}

func (s *ServiceManager) GetServiceById(id string) *Service {
	for _, service := range s.services {
		if service.Id == id {
			return service
		}
	}

	return nil
}

type scanService struct {
	Nid                uint16 `json:"nid"`
	Tsid               uint16 `json:"tsid"`
	Sid                uint16 `json:"sid"`
	Name               string `json:"name"`
	Type               uint8  `json:"type"`
	LogoId             uint64 `json:"logoId"`
	RemoteControlKeyId uint8  `json:"remoteControlKeyId"`
}

func (s *ServiceManager) ScanServices(ctx context.Context, streamManager *stream.StreamManager, channelType string, channelId string) error {
	out := bytes.Buffer{}

	session, err := streamManager.GetOrCreate(ctx, channelType, channelId)
	if err != nil {
		return err
	}
	if err := session.ScanServices(ctx, &out); err != nil {
		return err
	}

	var services []*scanService
	if err := json.Unmarshal(out.Bytes(), &services); err != nil {
		return err
	}

	s.services = make([]*Service, len(services))
	for i, service := range services {
		s.services[i] = &Service{
			Id:                 fmt.Sprintf("%05d%05d", service.Nid, service.Sid),
			ServiceId:          service.Sid,
			NetworkId:          service.Nid,
			TransportStreamId:  service.Tsid,
			Name:               service.Name,
			Type:               service.Type,
			RemoteControlKeyId: service.RemoteControlKeyId,
			ChannelType:        channelType,
			ChannelId:          channelId,
		}
	}

	return nil
}
