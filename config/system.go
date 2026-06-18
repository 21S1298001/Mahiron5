package config

import (
	"errors"
	"os"

	"sigs.k8s.io/yaml"
)

type SystemConfig struct {
	Addresses        []ServerAddress     `json:"addresses"`
	LogLevel         string              `json:"logLevel,omitempty"`
	JobMaxRunning    int                 `json:"jobMaxRunning,omitempty"`
	Jobs             []JobScheduleConfig `json:"jobs,omitempty"`
	DatabasePath     string              `json:"databasePath,omitempty"`
	EpgRetentionDays int                 `json:"epgRetentionDays,omitempty"`
}

type JobScheduleConfig struct {
	Key      string `json:"key"`
	Schedule string `json:"schedule"`
}

type ServerAddress struct {
	Http string `json:"http,omitempty"`
	Unix string `json:"unix,omitempty"`
}

func LoadAndParseSystemConfig(filePath string) (*SystemConfig, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	config := SystemConfig{
		DatabasePath:     "./mahiron.db",
		EpgRetentionDays: 3,
	}
	err = yaml.Unmarshal(file, &config)
	if err != nil {
		return nil, err
	}

	if len(config.Addresses) == 0 {
		config.Addresses = []ServerAddress{
			{
				Http: "localhost:40772",
			},
		}
	}

	for _, addr := range config.Addresses {
		if addr.Http == "" && addr.Unix == "" {
			return nil, errors.New("at least one address is required")
		}
		if addr.Http != "" && addr.Unix != "" {
			return nil, errors.New("only one address type is allowed")
		}
	}

	if config.LogLevel == "" {
		config.LogLevel = "info"
	}

	switch config.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		return nil, errors.New("invalid log level")
	}
	if config.JobMaxRunning == 0 {
		config.JobMaxRunning = 1
	}
	if config.JobMaxRunning < 1 || config.JobMaxRunning > 100 {
		return nil, errors.New("jobMaxRunning must be between 1 and 100")
	}

	if config.EpgRetentionDays < 0 {
		return nil, errors.New("epgRetentionDays must be >= 0 (0 = unlimited)")
	}

	return &config, nil
}
