package cmd

import "encoding/json"

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

	return c
}

func (c Config) Serialize() string {
	b, _ := json.MarshalIndent(c, "", "  ")

	return string(b)
}
