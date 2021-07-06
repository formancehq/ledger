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
		PostgresOpts struct {
		}
	} `json:"storage"`
}

type Overrides map[string]interface{}

func DefaultConfig() Config {
	c := Config{}

	h, err := os.UserHomeDir()

	if err != nil {
		panic("cannot get home directory")
	}

	p := path.Join(h, ".numary")

	c.Server.Http.BindAddress = "127.0.0.1:3068"
	c.Storage.Driver = "sqlite"
	c.Storage.SQLiteOpts.DBName = "ledger"
	c.Storage.SQLiteOpts.Directory = path.Join(p, "storage")

	return c
}

func (c Config) Serialize() string {
	b, _ := json.MarshalIndent(c, "", "  ")

	return string(b)
}

func GetConfig(overrides *Overrides) Config {
	candidates := []string{
		path.Join("/etc/numary", filename),
	}

	h, err := os.UserHomeDir()

	if err != nil {
		panic("cannot get home directory")
	}

	p := path.Join(h, ".numary")

	if _, err := os.Stat(p); os.IsNotExist(err) {
		err := os.Mkdir(p, 0700)

		if err != nil {
			panic(err)
		}
	}

	ps := path.Join(p, "storage")

	if _, err := os.Stat(ps); os.IsNotExist(err) {
		err := os.Mkdir(ps, 0700)

		if err != nil {
			panic(err)
		}
	}

	candidates = append(
		candidates,
		path.Join(h, ".numary", filename),
	)

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

	if addr, ok := (*overrides)["http-bind-addr"]; ok {
		conf.Server.Http.BindAddress = addr.(string)
	}

	return conf
}
