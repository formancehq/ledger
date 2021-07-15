package config

import (
	"os"
	"path"
	"strings"

	"github.com/spf13/viper"
)

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
	viper.SetDefault("server.http.bind_address", "localhost:3068")

	viper.SetConfigName("numary")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME/.numary")
	viper.AddConfigPath("/etc/numary")

	viper.SetEnvPrefix("numary")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))
	viper.AutomaticEnv()
}
