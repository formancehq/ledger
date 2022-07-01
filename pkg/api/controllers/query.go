package controllers

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
)

const (
	MaxPageSize     = 1000
	DefaultPageSize = storage.QueryDefaultPageSize
)

var (
	ErrInvalidPageSize = ledger.NewValidationError("invalid query value 'page_size'")
)

func getPageSize(c *gin.Context) (uint, error) {
	var (
		pageSize uint64
		err      error
	)
	if pageSizeParam := c.Query("page_size"); pageSizeParam != "" {
		pageSize, err = strconv.ParseUint(pageSizeParam, 10, 64)
		if err != nil {
			return 0, ErrInvalidPageSize
		}

		if pageSize > MaxPageSize {
			pageSize = MaxPageSize
		}
	} else {
		pageSize = DefaultPageSize
	}
	return uint(pageSize), nil
}
