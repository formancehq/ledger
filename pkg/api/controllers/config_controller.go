package controllers

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"net/http"

	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"gopkg.in/yaml.v3"
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

//go:embed swagger.yaml
var swagger string

func parseSwagger(version string) map[string]interface{} {
	ret := make(map[string]interface{})
	err := yaml.NewDecoder(bytes.NewBufferString(swagger)).Decode(&ret)
	if err != nil {
		panic(err)
	}
	ret["info"].(map[string]interface{})["version"] = version
	return ret
}

func (ctl *ConfigController) GetDocsAsYaml(w http.ResponseWriter, r *http.Request) {
	err := yaml.NewEncoder(w).Encode(parseSwagger(ctl.Version))
	if err != nil {
		panic(err)
	}
}

func (ctl *ConfigController) GetDocsAsJSON(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	err := enc.Encode(parseSwagger(ctl.Version))
	if err != nil {
		panic(err)
	}
}
