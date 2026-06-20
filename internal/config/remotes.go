package config

import (
	"errors"
	"os"

	"sigs.k8s.io/yaml"
)

type RemotesConfig []RemoteConfig

type RemoteConfig struct {
	Name      string           `json:"name"`
	URL       string           `json:"url"`
	BasicAuth *BasicAuthConfig `json:"basicAuth,omitempty"`
}

type BasicAuthConfig struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

func LoadAndParseRemotesConfig(filePath string) (RemotesConfig, error) {
	file, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	var config RemotesConfig
	if err := yaml.UnmarshalStrict(file, &config); err != nil {
		return nil, err
	}

	seen := map[string]struct{}{}
	for _, remote := range config {
		if remote.Name == "" {
			return nil, errors.New("remote name is required")
		}
		if _, ok := seen[remote.Name]; ok {
			return nil, errors.New("duplicate remote name")
		}
		seen[remote.Name] = struct{}{}
		if remote.URL == "" {
			return nil, errors.New("remote url is required")
		}
		if remote.BasicAuth != nil && (remote.BasicAuth.Username == "" || remote.BasicAuth.Password == "") {
			return nil, errors.New("remote basicAuth username and password are required")
		}
	}

	return config, nil
}

func (c RemotesConfig) Get(name string) *RemoteConfig {
	for i := range c {
		if c[i].Name == name {
			return &c[i]
		}
	}
	return nil
}
