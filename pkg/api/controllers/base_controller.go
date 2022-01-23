package controllers

import (
	"reflect"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/ledger/query"
)

// Controllers struct
type BaseController struct{}

type BaseResponse struct {}

func (ctl *BaseController) response(c *gin.Context, status int, data interface{}) {
	if data == nil {
		c.Status(status)
	}
	if reflect.TypeOf(data) == reflect.TypeOf(query.Cursor{}) {
		c.JSON(status, gin.H{
			"cursor": data,
		})
	} else {
		c.JSON(status, gin.H{
			"data": data,
		})
	}
}

func (ctl *BaseController) responseError(c *gin.Context, status int, err error) {
	c.Abort()
	c.AbortWithStatusJSON(status, gin.H{
		"error":         true,
		"error_code":    status,
		"error_message": err.Error(),
	})
}
