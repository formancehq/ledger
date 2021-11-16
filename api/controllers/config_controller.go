package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/api/schemas"
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
	ctl.success(
		c,
		http.StatusOK,
		ctl.configService.GetConfig(),
		&schemas.Infos{},
	)
}
