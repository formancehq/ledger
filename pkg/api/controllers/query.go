package controllers

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/ledger"
)

const (
	MaxPageSize     = 1000
	DefaultPageSize = ledger.QueryDefaultPageSize

	QueryKeyCursor = "cursor"
	// Deprecated
	QueryKeyCursorDeprecated = "pagination_token"
)

var (
	ErrInvalidPageSize = ledger.NewValidationError("invalid 'page_size' query param")
)

func getPageSize(c *gin.Context) (uint, error) {
	pageSizeParam := c.Query("page_size")
	if pageSizeParam == "" {
		return DefaultPageSize, nil
	}

	pageSize, err := strconv.ParseUint(pageSizeParam, 10, 32)
	if err != nil {
		return 0, ErrInvalidPageSize
	}

	if pageSize > MaxPageSize {
		return MaxPageSize, nil
	}

	return uint(pageSize), nil
}
