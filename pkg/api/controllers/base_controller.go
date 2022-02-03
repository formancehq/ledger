package controllers

import (
	"net/http"
	"reflect"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/ledger/query"
)

// Controllers struct
type BaseController struct{}

type BaseResponse struct {
	Data   interface{} `json:"data,omitempty"`
	Cursor interface{} `json:"cursor,omitempty"`
}

func (ctl *BaseController) response(c *gin.Context, status int, data interface{}) {
	if data == nil {
		c.Status(status)
	}
	if reflect.TypeOf(data) == reflect.TypeOf(query.Cursor{}) {
		c.JSON(status, BaseResponse{
			Cursor: data,
		})
	} else {
		c.JSON(status, BaseResponse{
			Data: data,
		})
	}
}

const (
	ErrInternal         = "INTERNAL"
	ErrConflict         = "CONFLICT"
	ErrInsufficientFund = "INSUFFICIENT_FUND"
	ErrValidation       = "VALIDATION"
	ErrNotFound         = "NOT_FOUND"
)

type ErrorResponse struct {
	ErrorCode    string `json:"error_code,omitempty" enums:"INTERNAL,CONFLICT,INSUFFICIENT_FUND,VALIDATION,NOT_FOUND"`
	ErrorMessage string `json:"error_message,omitempty"`
}

func (ctl *BaseController) noContent(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

func (ctl *BaseController) responseError(c *gin.Context, status int, code string, err error) {
	c.Abort()
	c.AbortWithStatusJSON(status, ErrorResponse{
		ErrorCode:    code,
		ErrorMessage: err.Error(),
	})
}
