package service

import "github.com/21S1298001/mahiron/internal/config"

type Service struct {
	Id                  string
	ServiceId           uint16
	NetworkId           uint16
	TransportStreamId   uint16
	Name                string
	Type                uint8
	EITScheduleFlag     bool
	EITPresentFollowing bool
	LogoId              *int64
	LogoVersion         *int64
	LogoDownloadDataId  *int64
	HasLogoData         bool
	RemoteControlKeyId  uint8
	ChannelType         string
	ChannelId           string
	EPG                 EPGStatus
}

type EPGStatus struct {
	LastAttemptAt *int64
	LastSuccessAt *int64
	LastError     string
}

type LogoTarget struct {
	NetworkId          uint16
	ServiceId          uint16
	TransportStreamId  uint16
	ChannelType        string
	ChannelId          string
	LogoId             int64
	LogoVersion        int64
	LogoDownloadDataId int64
	IsCommonData       bool
}

func (s *Service) ItemId() int64 {
	return int64(s.NetworkId)*100000 + int64(s.ServiceId)
}

func (s *Service) EventData(channel *config.ChannelConfig) map[string]any {
	data := map[string]any{
		"id":                  s.ItemId(),
		"serviceId":           s.ServiceId,
		"networkId":           s.NetworkId,
		"transportStreamId":   s.TransportStreamId,
		"name":                s.Name,
		"type":                int(s.Type),
		"eitScheduleFlag":     s.EITScheduleFlag,
		"eitPresentFollowing": s.EITPresentFollowing,
		"hasLogoData":         s.HasLogoData,
		"remoteControlKeyId":  int(s.RemoteControlKeyId),
	}
	if s.LogoId != nil {
		data["logoId"] = *s.LogoId
	}
	if s.EPG.LastSuccessAt != nil {
		data["epgReady"] = true
		data["epgUpdatedAt"] = *s.EPG.LastSuccessAt
	} else {
		data["epgReady"] = false
	}
	if s.EPG.LastAttemptAt != nil {
		data["epgLastAttemptAt"] = *s.EPG.LastAttemptAt
	}
	if s.EPG.LastError != "" {
		data["epgLastError"] = s.EPG.LastError
	}
	if channel != nil {
		channelData := map[string]any{
			"type":    channel.Type,
			"channel": channel.Channel,
			"name":    channel.Name,
		}
		if channel.TsmfRelTs != nil {
			channelData["tsmfRelTs"] = *channel.TsmfRelTs
		}
		data["channel"] = channelData
	}
	return data
}
