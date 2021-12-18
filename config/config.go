package config

import (
	"github.com/sirupsen/logrus"
	"os"
	"path"
	"strings"

	"github.com/spf13/viper"
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
	Driver  interface{} `json:"driver"`
	Ledgers interface{} `json:"ledgers"`
}

func Init() {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "/root"
	}

	os.MkdirAll(path.Join(home, ".numary", "data"), 0700)

	viper.SetDefault("debug", false)
	viper.SetDefault("storage.driver", "sqlite")
	viper.SetDefault("storage.dir", path.Join(home, ".numary/data"))
	viper.SetDefault("storage.sqlite.db_name", "numary")
	viper.SetDefault("storage.postgres.conn_string", "postgresql://localhost/postgres")
	viper.SetDefault("storage.cache", false)
	viper.SetDefault("server.http.bind_address", "localhost:3068")
	viper.SetDefault("ui.http.bind_address", "localhost:3078")
	viper.SetDefault("ledgers", []string{"quickstart"})

	viper.SetConfigName("numary")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME/.numary")
	viper.AddConfigPath("/etc/numary")
	viper.ReadInConfig()

	viper.SetEnvPrefix("numary")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}

func Remember(ledger string) {
	ledgers := viper.GetStringSlice("ledgers")

	for _, v := range ledgers {
		if ledger == v {
			return
		}
	}

	viper.Set("ledgers", append(ledgers, ledger))

	err := viper.WriteConfig()

	if err != nil {
		logrus.Printf(
			"failed to write config: ledger %s will not be remembered\n",
			ledger,
		)
	}
}
