package controllers

import (
	"context"
	"net/http"
	"reflect"

	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

type BaseController struct{}

func (ctl *BaseController) response(c *gin.Context, status int, data interface{}) {
	if data == nil {
		c.Status(status)
	}
	if reflect.TypeOf(data) == reflect.TypeOf(sharedapi.Cursor{}) {
		cursor := data.(sharedapi.Cursor)
		c.JSON(status, sharedapi.BaseResponse{
			Cursor: &cursor,
		})
	} else {
		c.JSON(status, sharedapi.BaseResponse{
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
	case errors.Is(err, context.Canceled):
		return http.StatusInternalServerError, ErrContextCancelled
	case storage.IsError(err):
		return http.StatusServiceUnavailable, ErrStore
	default:
		return http.StatusInternalServerError, ErrInternal
	}
}

func ErrorCode(c *gin.Context) string {
	return c.GetString(errorCodeKey)
}

func ResponseError(c *gin.Context, err error) {
	_ = c.Error(err)
	status, code := coreErrorToErrorCode(err)
	c.Set(errorCodeKey, code)

	if status < 500 {
		c.AbortWithStatusJSON(status, sharedapi.ErrorResponse{
			ErrorCode:    code,
			ErrorMessage: err.Error(),
		})
		return
	}
	c.AbortWithStatus(status)
}
