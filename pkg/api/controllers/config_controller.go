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

type ConfigController struct {
	Version       string
	StorageDriver storage.Driver
}

func NewConfigController(version string, storageDriver storage.Driver) ConfigController {
	return ConfigController{
		Version:       version,
		StorageDriver: storageDriver,
	}
}

func (ctl *ConfigController) GetInfo(w http.ResponseWriter, r *http.Request) {
	ledgers, err := ctl.StorageDriver.GetSystemStore().ListLedgers(r.Context())
	if err != nil {
		panic(err)
	}

	sharedapi.RawOk(w, ConfigInfo{
		Server:  "ledger",
		Version: ctl.Version,
		Config: &Config{
			LedgerStorage: &LedgerStorage{
				Driver:  ctl.StorageDriver.Name(),
				Ledgers: ledgers,
			},
		},
	})
}
