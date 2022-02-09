package controllers

import (
	"context"
	"errors"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
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
	ErrContextCancelled = "CONTEXT_CANCELLED"
	ErrStore            = "STORE"

	errorCodeKey = "_errorCode"
)

type ErrorResponse struct {
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

func (ctl *BaseController) noContent(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

func coreErrorToErrorCode(err error) (int, string) {
	switch {
	case ledger.IsConflictError(err):
		return http.StatusConflict, ErrConflict
	case ledger.IsInsufficientFundError(err):
		return http.StatusBadRequest, ErrInsufficientFund
	case ledger.IsValidationError(err):
		return http.StatusBadRequest, ErrValidation
	case storage.IsError(err):
		return http.StatusServiceUnavailable, ErrStore
	case errors.Is(err, context.Canceled):
		return http.StatusInternalServerError, ErrContextCancelled
	default:
		return http.StatusInternalServerError, ErrInternal
	}
}

func ErrorCode(c *gin.Context) string {
	return c.GetString(errorCodeKey)
}

func ResponseError(c *gin.Context, err error) {
	c.Error(err)
	status, code := coreErrorToErrorCode(err)
	c.Set(errorCodeKey, code)

	if status < 500 {
		c.AbortWithStatusJSON(status, ErrorResponse{
			ErrorCode:    code,
			ErrorMessage: err.Error(),
		})
		return
	}
	c.AbortWithStatus(status)
}
