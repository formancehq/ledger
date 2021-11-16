package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/api/resources"
	"github.com/numary/ledger/api/services"
)

// ConfigController -
type ConfigController struct {
	BaseController
	configService *services.ConfigService
}

// NewConfigController -
func NewConfigController(
	configService *services.ConfigService,
) *ConfigController {
	return &ConfigController{
		configService: configService,
	}
}

// CreateConfigController -
func CreateConfigController() *ConfigController {
	return NewConfigController(
		services.CreateConfigService(),
	)
}

// GetInfo -
func (ctl *ConfigController) GetInfo(c *gin.Context) {
	info := ctl.configService.GetConfig()
	ctl.responseResource(
		c,
		http.StatusOK,
		info,
		&resources.Info{},
	)
}
