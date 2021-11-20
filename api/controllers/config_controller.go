package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/config"
	"github.com/spf13/viper"
)

// ConfigController -
type ConfigController struct {
	BaseController
}

// NewConfigController -
func NewConfigController() ConfigController {
	return ConfigController{}
}

// GetInfo -
func (ctl *ConfigController) GetInfo(c *gin.Context) {
	ctl.response(
		c,
		http.StatusOK,
		config.ConfigInfo{
			Server:  "numary-ledger",
			Version: viper.Get("version"),
			Config: &config.Config{
				LedgerStorage: &config.LedgerStorage{
					Driver:  viper.Get("storage.driver"),
					Ledgers: viper.Get("ledgers"),
				},
			},
		},
	)
}
