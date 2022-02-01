package controllers

import (
	"bytes"
	"gopkg.in/yaml.v2"
	"net/http"

	_ "embed"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/config"
)

type LedgerLister interface {
	List(r *http.Request) []string
}
type LedgerListerFn func(r *http.Request) []string

func (fn LedgerListerFn) List(r *http.Request) []string {
	return fn(r)
}

// ConfigController -
type ConfigController struct {
	BaseController
	Version       string
	StorageDriver string
	LedgerLister  LedgerLister
}

// NewConfigController -
func NewConfigController(version string, storageDriver string, lister LedgerLister) ConfigController {
	return ConfigController{
		Version:       version,
		StorageDriver: storageDriver,
		LedgerLister:  lister,
	}
}

func (ctl *ConfigController) GetInfo(c *gin.Context) {
	ctl.response(
		c,
		http.StatusOK,
		config.ConfigInfo{
			Server:  "numary-ledger",
			Version: ctl.Version,
			Config: &config.Config{
				LedgerStorage: &config.LedgerStorage{
					Driver:  ctl.StorageDriver,
					Ledgers: ctl.LedgerLister.List(c.Request),
				},
			},
		},
	)
}

//go:embed swagger.yaml
var swagger string

func (ctl *ConfigController) GetDocsAsYaml(c *gin.Context) {
	c.Writer.Write([]byte(swagger))
}

func (ctl *ConfigController) GetDocsAsJSON(c *gin.Context) {
	ret := make(map[string]interface{})
	err := yaml.NewDecoder(bytes.NewBufferString(swagger)).Decode(&ret)
	if err != nil {
		panic(err)
	}

	c.JSON(http.StatusOK, ret)
}
