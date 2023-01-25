package fctl

import (
	"encoding/json"
	"os"
	"path"
)

const (
	DefaultMembershipURI = "https://app.formance.cloud/api"
)

type ConfigManager struct {
	configFilePath string
}

func (m *ConfigManager) Load() (*Config, error) {
	f, err := os.Open(m.configFilePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &Config{
				profiles: map[string]*Profile{},
				manager:  m,
			}, nil
		}
		return nil, err
	}
	defer f.Close()

	cfg := &Config{}
	if err := json.NewDecoder(f).Decode(cfg); err != nil {
		return nil, err
	}
	cfg.manager = m
	if cfg.profiles == nil {
		cfg.profiles = map[string]*Profile{}
	}

	return cfg, nil
}

func (m *ConfigManager) UpdateConfig(config *Config) error {
	if err := os.MkdirAll(path.Dir(m.configFilePath), 0700); err != nil {
		return err
	}

	f, err := os.OpenFile(m.configFilePath, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	enc := json.NewEncoder(f)
	enc.SetIndent("", "  ")
	if err := enc.Encode(config); err != nil {
		return err
	}
	return nil
}

func NewConfigManager(configFilePath string) *ConfigManager {
	return &ConfigManager{
		configFilePath: configFilePath,
	}
}
