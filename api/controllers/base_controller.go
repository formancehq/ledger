package controllers

import (
	"reflect"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/ledger/query"
)

// Controllers struct
type BaseController struct{}

func (ctl *BaseController) response(c *gin.Context, status int, data interface{}) {
	if data == nil {
		c.Status(status)
	}
	if reflect.TypeOf(data) == reflect.TypeOf(query.Cursor{}) {
		c.JSON(status, gin.H{
			"ok":     true,
			"cursor": data,
		})
	} else {
		c.JSON(status, gin.H{
			"ok":   true,
			"data": data,
		})
	}
}

func (ctl *BaseController) responseError(c *gin.Context, status int, err error) {
	c.Abort()
	c.AbortWithStatusJSON(status, gin.H{
		"ok":            false,
		"error":         true,
		"error_code":    status,
		"error_message": err.Error(),
	})
}
