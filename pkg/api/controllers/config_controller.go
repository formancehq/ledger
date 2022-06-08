package controllers

import (
	"bytes"
	_ "embed"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
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
	StorageDriver storage.Driver
}

func NewConfigController(version string, storageDriver storage.Driver) ConfigController {
	return ConfigController{
		Version:       version,
		StorageDriver: storageDriver,
	}
}

func (ctl *ConfigController) GetInfo(c *gin.Context) {
	ledgers, err := ctl.StorageDriver.List(c.Request.Context())
	if err != nil {
		panic(err)
	}
	c.JSON(http.StatusOK, ConfigInfo{
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

func (ctl *ConfigController) GetDocsAsYaml(c *gin.Context) {
	err := yaml.NewEncoder(c.Writer).Encode(parseSwagger(ctl.Version))
	if err != nil {
		panic(err)
	}
}

func (ctl *ConfigController) GetDocsAsJSON(c *gin.Context) {
	enc := json.NewEncoder(c.Writer)
	enc.SetIndent("", "  ")
	err := enc.Encode(parseSwagger(ctl.Version))
	if err != nil {
		panic(err)
	}
}
