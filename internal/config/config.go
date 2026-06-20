package config

type Config struct {
	Channels ChannelsConfig
	System   *SystemConfig
	Tuners   TunersConfig
}

func LoadAndParseConfig() (*Config, error) {
	channels, err := LoadAndParseChannelsConfig("channels.yml")
	if err != nil {
		return nil, err
	}

	system, err := LoadAndParseSystemConfig("server.yml")
	if err != nil {
		return nil, err
	}

	tunres, err := LoadAndParseTunersConfig("tuners.yml")
	if err != nil {
		return nil, err
	}

	return &Config{
		Channels: channels,
		System:   system,
		Tuners:   tunres,
	}, nil
}
