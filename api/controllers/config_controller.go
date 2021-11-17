package controllers

import (
	"github.com/gin-gonic/gin"
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
	// info := ctl.configService.GetConfig()
	// ctl.responseResource(
	// 	c,
	// 	http.StatusOK,
	// 	info,
	// 	&resources.Info{},
	// )
}
