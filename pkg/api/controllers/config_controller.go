package controllers

import (
	_ "embed"
	"net/http"

	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
)

type ConfigInfo struct {
	Server  string  `json:"server"`
	Version string  `json:"version"`
	Config  *Config `json:"config"`
}

type Config struct {
	LedgerStorage *LedgerStorage `json:"storage"`
}

type LedgerStorage struct {
	Driver  string   `json:"driver"`
	Ledgers []string `json:"ledgers"`
}

func GetInfo(backend Backend) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ledgers, err := backend.ListLedgers(r.Context())
		if err != nil {
			panic(err)
		}

		sharedapi.RawOk(w, ConfigInfo{
			Server:  "ledger",
			Version: backend.GetVersion(),
			Config: &Config{
				LedgerStorage: &LedgerStorage{
					Driver:  "postgres",
					Ledgers: ledgers,
				},
			},
		})
	}
}
