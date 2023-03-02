package controllers

import (
	_ "embed"
	"net/http"

	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
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
	StorageDriver storage.Driver[ledger.Store]
}

func NewConfigController(version string, storageDriver storage.Driver[ledger.Store]) ConfigController {
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

	sharedapi.Ok(w, ConfigInfo{
		Server:  "numary-ledger",
		Version: ctl.Version,
		Config: &Config{
			LedgerStorage: &LedgerStorage{
				Driver:  ctl.StorageDriver.Name(),
				Ledgers: ledgers,
			},
		},
	})
}
