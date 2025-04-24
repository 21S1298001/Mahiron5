package service

type Service struct {
	Id                 string
	ServiceId          uint16
	NetworkId          uint16
	TransportStreamId  uint16
	Name               string
	Type               uint8
	RemoteControlKeyId uint8
	ChannelType        string
	ChannelId          string
}
