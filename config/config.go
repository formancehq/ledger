package config

import (
	"encoding/json"
	"os"
	"path"

	"github.com/kelseyhightower/envconfig"
	"github.com/spf13/viper"
)

type Config struct {
	Server struct {
		Http struct {
			BindAddress string `default:"127.0.0.1:3068" json:"bind_address"`
		} `json:"http"`
	} `json:"server"`
	Storage struct {
		Driver     string `default:"sqlite" json:"driver"`
		DataDir    string `default:"$HOME/.numary/storage"`
		SQLiteOpts struct {
			DBName string `default:"ledger" json:"db_name"`
		} `json:"sqlite_opts"`
		PostgresOpts struct {
			ConnString string `default:"postgresql://localhost/postgres" json:"conn_string"`
		} `json:"postgres_opts"`
	} `json:"storage"`
}

// func (c Config) Expand(path string) (string, error) {
// 	home, err := os.UserHomeDir()

// 	if err != nil {
// 		return "", err
// 	}

// 	return strings.Replace(home, "$HOME", home, -1), nil
// }

// if _, err := os.Stat(ps); os.IsNotExist(err) {
// 	err := os.Mkdir(ps, 0700)

// 	if err != nil {
// 		panic(err)
// 	}
// }

func Init() {
	home, err := os.UserHomeDir()
	if err != nil {
		home = "/root"
	}

	os.MkdirAll(path.Join(home, ".numary", "data"), 0700)

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
	viper.AutomaticEnv()
}

func DefaultConfig() Config {
	c := Config{}
	envconfig.Process("numary", &c)

	return c
}

func (c Config) Serialize() string {
	b, _ := json.MarshalIndent(c, "", "  ")

	return string(b)
}

func GetConfig() Config {
	return DefaultConfig()
}
