package config

import (
	"context"
	"github.com/numary/ledger/pkg/logging"
	"github.com/spf13/viper"
	"os"
	"path/filepath"
)

// ConfigInfo struct
type ConfigInfo struct {
	Server  string      `json:"server"`
	Version interface{} `json:"version"`
	Config  *Config     `json:"config"`
}

// Config struct
type Config struct {
	LedgerStorage *LedgerStorage `json:"storage"`
}

// LedgerStorage struct
type LedgerStorage struct {
	Driver  string   `json:"driver"`
	Ledgers []string `json:"ledgers"`
}

func Remember(ctx context.Context, logger logging.Logger, ledger string) {
	ledgers := viper.GetStringSlice("ledgers")

	for _, v := range ledgers {
		if ledger == v {
			return
		}
	}

	home, err := os.UserHomeDir()
	if err != nil {
		home = "/root"
	}

	writeTo := ""
	userConfigFile := filepath.Join(home, ".numary/numary.yaml")
	for _, file := range []string{"/etc/numary/numary.yaml", userConfigFile} {
		_, err := os.Open(file)
		if err == nil {
			writeTo = file
			break
		}
	}
	if writeTo == "" {
		_, err := os.Create(userConfigFile)
		if err != nil {
			logger.Error(ctx, "failed to create config file: ledger %s will not be remembered", ledger)
		}
	}

	viper.Set("ledgers", append(ledgers, ledger))
	err = viper.WriteConfig()
	if err != nil {
		logger.Error(ctx, "failed to write config: ledger %s will not be remembered",
			ledger)
	}
}
