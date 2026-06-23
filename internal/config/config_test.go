package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadAndParseConfigFromDir(t *testing.T) {
	configDir := t.TempDir()
	writeConfigFile(t, configDir, "channels.yml", `
- name: NHK
  type: GR
  channel: "27"
`)
	writeConfigFile(t, configDir, "server.yml", "")
	writeConfigFile(t, configDir, "tuners.yml", `
- name: Tuner1
  types:
    - GR
  command: cat
`)

	cfg, err := LoadAndParseConfigFromDir(configDir)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Channels) != 1 || cfg.Channels[0].Name != "NHK" {
		t.Fatalf("channels not loaded from config dir: %#v", cfg.Channels)
	}
	if len(cfg.Tuners) != 1 || cfg.Tuners[0].Name != "Tuner1" {
		t.Fatalf("tuners not loaded from config dir: %#v", cfg.Tuners)
	}
	if cfg.System == nil || cfg.System.LogLevel != "info" {
		t.Fatalf("system config not loaded with defaults: %#v", cfg.System)
	}
	if cfg.Remotes != nil {
		t.Fatalf("missing remotes.yml without remote routes should be allowed: %#v", cfg.Remotes)
	}
}

func TestLoadAndParseConfigFromDirRequiresRemotesForRemoteRoutes(t *testing.T) {
	configDir := t.TempDir()
	writeConfigFile(t, configDir, "channels.yml", `
- name: NHK
  type: GR
  channel: "27"
  routes:
    - id: remote
      remote: living
      type: GR
      channel: "27"
`)
	writeConfigFile(t, configDir, "server.yml", "")
	writeConfigFile(t, configDir, "tuners.yml", `
- name: Tuner1
  types:
    - GR
  command: cat
`)

	if _, err := LoadAndParseConfigFromDir(configDir); err == nil {
		t.Fatal("missing remotes.yml with remote routes should fail")
	}
}

func writeConfigFile(t *testing.T, dir, name, contents string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, name), []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
}
