package config

import (
	"errors"
	"os"

	"sigs.k8s.io/yaml"
)

type ChannelsConfig []ChannelConfig

type ChannelConfig struct {
	// https://github.com/Chinachu/Mirakurun/blob/61c4155d2535c56fbf6fd379c5e8aba779fd642b/api.d.ts#L320
	Name        string         `json:"name"`
	Type        string         `json:"type"`
	Channel     string         `json:"channel"`
	ServiceId   *uint32        `json:"serviceId,omitempty"`
	TsmfRelTs   *uint8         `json:"tsmfRelTs,omitempty"`
	CommandVars map[string]any `json:"commandVars,omitempty"`
	IsDisabled  *bool          `json:"isDisabled,omitempty"`
	Satelite    *string        `json:"satelite,omitempty"`  // deprecated
	Satellite   *string        `json:"satellite,omitempty"` // deprecated
	Space       *uint8         `json:"space,omitempty"`     // deprecated
	Freq        *uint32        `json:"freq,omitempty"`      // deprecated
	Polarity    *string        `json:"polarity,omitempty"`  // deprecated

	// Mahiron extension
	TunerGroups []string `json:"tunerGroups,omitempty"`
}

func LoadAndParseChannelsConfig(filePath string) (ChannelsConfig, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var config ChannelsConfig
	err = yaml.Unmarshal(file, &config)
	if err != nil {
		return nil, err
	}
	if len(config) == 0 {
		return nil, errors.New("at least one channel is required")
	}

	no := false

	for i, channel := range config {
		if channel.Name == "" {
			return nil, errors.New("channel name is required")
		}
		if channel.Type == "" {
			return nil, errors.New("channel type is required")
		}
		if channel.Channel == "" {
			return nil, errors.New("channel symbol is required")
		}
		if channel.TsmfRelTs != nil && channel.ServiceId == nil {
			return nil, errors.New("serviceId is required when tsmfRelTs is set")
		}
		if channel.TsmfRelTs != nil && *channel.TsmfRelTs > 0x0F {
			return nil, errors.New("tsmfRelTs must be between 0 and 15")
		}
		if channel.CommandVars != nil && (channel.Satelite != nil ||
			channel.Satellite != nil ||
			channel.Space != nil ||
			channel.Freq != nil ||
			channel.Polarity != nil) {
			return nil, errors.New("commandVars cannot be used with satelite, satellite, space, freq, or polarity")
		}

		if channel.IsDisabled == nil {
			config[i].IsDisabled = &no
		}
		if channel.CommandVars == nil {
			config[i].CommandVars = make(map[string]any)
		}
		if channel.Satelite != nil {
			config[i].CommandVars["satellite"] = *channel.Satelite
			config[i].Satelite = nil
		}
		if channel.Satellite != nil {
			config[i].CommandVars["satellite"] = *channel.Satellite
			config[i].Satellite = nil
		}
		if channel.Space != nil {
			config[i].CommandVars["space"] = *channel.Space
			config[i].Space = nil
		}
		if channel.Freq != nil {
			config[i].CommandVars["freq"] = *channel.Freq
			config[i].Freq = nil
		}
		if channel.Polarity != nil {
			config[i].CommandVars["polarity"] = *channel.Polarity
			config[i].Polarity = nil
		}
		if channel.TunerGroups == nil {
			config[i].TunerGroups = []string{}
		}
	}

	return config, nil
}
