package controllers

import (
	"reflect"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/ledger/query"
)

// Controllers struct
type BaseController struct{}

type BaseResponse struct{}

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

type Error struct {
	ErrorCode    string `json:"error_code"`
	ErrorMessage string `json:"error_message"`
}

func (ctl *BaseController) responseError(c *gin.Context, status int, code string, err error) {
	c.Abort()
	c.AbortWithStatusJSON(status, Error{
		ErrorCode:    code,
		ErrorMessage: err.Error(),
	})
}
