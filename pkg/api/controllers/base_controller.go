package controllers

import (
	"context"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
)

func respondWithNoContent(c *gin.Context) {
	c.Status(http.StatusNoContent)
}

func respondWithCursor[T any](c *gin.Context, status int, data sharedapi.Cursor[T]) {
	c.JSON(status, sharedapi.BaseResponse[T]{
		Cursor: &data,
	})
}

func respondWithData[T any](c *gin.Context, status int, data T) {
	c.JSON(status, sharedapi.BaseResponse[T]{
		Data: &data,
	})
}

const (
	ErrInternal         = "INTERNAL"
	ErrConflict         = "CONFLICT"
	ErrInsufficientFund = "INSUFFICIENT_FUND"
	ErrValidation       = "VALIDATION"
	ErrContextCancelled = "CONTEXT_CANCELLED"
	ErrStore            = "STORE"
	ErrNotFound         = "NOT_FOUND"

	errorCodeKey = "_errorCode"
)

func coreErrorToErrorCode(err error) (int, string) {
	switch {
	case ledger.IsConflictError(err):
		return http.StatusConflict, ErrConflict
	case ledger.IsInsufficientFundError(err):
		return http.StatusBadRequest, ErrInsufficientFund
	case ledger.IsValidationError(err):
		return http.StatusBadRequest, ErrValidation
	case ledger.IsNotFoundError(err):
		return http.StatusNotFound, ErrNotFound
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
	} else {
		c.AbortWithStatus(status)
	}
}
