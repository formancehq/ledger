package config

import (
	"log"
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
	viper.SetDefault("ui.http.bind_address", "localhost:3078")
	viper.SetDefault("ledgers", []interface{}{"quickstart"})

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
	ledgers := viper.Get("ledgers").([]interface{})

	for _, v := range ledgers {
		if ledger == v.(string) {
			return
		}
	}

	viper.Set("ledgers", append(ledgers, ledger))

	err := viper.WriteConfig()

	if err != nil {
		log.Printf(
			"failed to write config: ledger %s will not be remembered\n",
			ledger,
		)
	}
}
