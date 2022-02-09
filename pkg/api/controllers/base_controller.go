package controllers

import (
	"context"
	"errors"
	"github.com/numary/ledger/pkg/ledger"
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

	errorCodeKey = "_errorCode"
)

type ErrorResponse struct {
	ErrorCode    string `json:"error_code,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`
}

func (ctl *BaseController) noContent(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

func coreErrorToErrorCode(err error) (int, string, bool) {
	switch {
	case ledger.IsConflictError(err):
		return http.StatusConflict, ErrConflict, true
	case ledger.IsInsufficientFundError(err):
		return http.StatusBadRequest, ErrInsufficientFund, true
	case ledger.IsValidationError(err):
		return http.StatusBadRequest, ErrValidation, true
	case ledger.IsUnavailableStoreError(err):
		return http.StatusServiceUnavailable, ErrInternal, false
	case errors.Is(err, context.Canceled):
		return http.StatusInternalServerError, ErrContextCancelled, false
	default:
		return http.StatusInternalServerError, ErrInternal, false
	}
}

func ErrorCode(c *gin.Context) string {
	return c.GetString(errorCodeKey)
}

func ResponseError(c *gin.Context, err error) {
	c.Error(err)
	status, code, exposeMessage := coreErrorToErrorCode(err)
	message := ""
	if exposeMessage {
		message = err.Error()
	}
	c.Set(errorCodeKey, code)
	c.AbortWithStatusJSON(status, ErrorResponse{
		ErrorCode:    code,
		ErrorMessage: message,
	})
}
