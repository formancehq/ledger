package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/spf13/viper"
)

// BaseController -
type BaseController struct {
}

// NewBaseController -
func NewBaseController() *BaseController {
	return &BaseController{}
}

// CreateBaseController -
func CreateBaseController() *BaseController {
	return NewBaseController()
}

// GetStats -
func (ctl *BaseController) GetInfos(c *gin.Context) {
	c.JSON(200, gin.H{
		"server":  "numary-ledger",
		"version": viper.Get("version"),
		"config": gin.H{
			"storage": gin.H{
				"driver": viper.Get("storage.driver"),
			},
			"ledgers": viper.Get("ledgers"),
		},
	})
}
