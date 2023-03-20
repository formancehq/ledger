package controllers

import (
	_ "embed"
	"net/http"

	"github.com/formancehq/ledger/pkg/storage"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
)

type ConfigInfo struct {
	Server  string      `json:"server"`
	Version interface{} `json:"version"`
	Config  *Config     `json:"config"`
}

type Config struct {
	LedgerStorage *LedgerStorage `json:"storage"`
}

type LedgerStorage struct {
	Driver  string   `json:"driver"`
	Ledgers []string `json:"ledgers"`
}

func GetInfo(storageDriver storage.Driver, version string) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		ledgers, err := storageDriver.GetSystemStore().ListLedgers(r.Context())
		if err != nil {
			panic(err)
		}

		sharedapi.RawOk(w, ConfigInfo{
			Server:  "ledger",
			Version: version,
			Config: &Config{
				LedgerStorage: &LedgerStorage{
					Driver:  storageDriver.Name(),
					Ledgers: ledgers,
				},
			},
		})
	}
}
