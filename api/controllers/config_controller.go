package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/api/models"
	"github.com/numary/ledger/api/resources"
	"github.com/spf13/viper"
)

// ConfigController -
type ConfigController struct {
	BaseController
}

// NewConfigController -
func NewConfigController() *ConfigController {
	return &ConfigController{}
}

// GetInfo -
func (ctl *ConfigController) GetInfo(c *gin.Context) {
	ctl.responseResource(
		c,
		http.StatusOK,
		&models.Infos{
			Server:  "numary-ledger",
			Version: viper.Get("version"),
			Config: &models.Config{
				LedgerStorage: &models.LedgerStorage{
					Driver:  viper.Get("storage.driver"),
					Ledgers: viper.Get("ledgers"),
				},
			},
		},
		&resources.Info{},
	)
}
