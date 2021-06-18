package config

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
)

const (
	filename = "numary.config.json"
)

type Config struct {
	Server struct {
		Http struct {
			BindAddress string `json:"bind_address"`
		} `json:"http"`
	} `json:"server"`
	Storage struct {
		Driver     string `json:"driver"`
		SQLiteOpts struct {
			Directory string `json:"directory"`
			DBName    string `json:"db_name"`
		} `json:"sqlite_opts"`
	} `json:"storage"`
}

func DefaultConfig() Config {
	c := Config{}

	c.Server.Http.BindAddress = "127.0.0.1:3068"
	c.Storage.Driver = "sqlite"
	c.Storage.SQLiteOpts.DBName = "ledger"
	c.Storage.SQLiteOpts.Directory = ".numary"

	return c
}

func (c Config) Serialize() string {
	b, _ := json.MarshalIndent(c, "", "  ")

	return string(b)
}

func GetConfig() Config {
	candidates := []string{
		path.Join("/etc/numary", filename),
	}

	h, err := os.UserHomeDir()

	if err == nil {
		candidates = append(
			candidates,
			path.Join(h, ".numary", filename),
		)
	}

	found := false
	conf := DefaultConfig()

	for _, c := range candidates {
		b, err := os.ReadFile(c)

		if err != nil {
			continue
		}

		err = json.Unmarshal(b, &conf)

		if err != nil {
			fmt.Printf("error parsing config %s", c)
			os.Exit(1)
		}

		found = true
	}

	if !found {
		fmt.Println("fallback to default config")
	}

	return conf
}
