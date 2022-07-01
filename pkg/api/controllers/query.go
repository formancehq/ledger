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
	ErrInvalidPageSize  = ledger.NewValidationError("invalid query value 'page_size'")
	ErrNegativePageSize = ledger.NewValidationError("cannot pass negative 'page_size'")
)

func getPageSize(c *gin.Context) (uint, error) {
	var (
		// Use int instead of uint because if the client pass a negative page_size
		// the uint will transparently convert the value to a valid uint
		// and the error will not be catched
		pageSize int64
		err      error
	)
	if pageSizeParam := c.Query("page_size"); pageSizeParam != "" {
		pageSize, err = strconv.ParseInt(pageSizeParam, 10, 64)
		if err != nil {
			return 0, ErrInvalidPageSize
		}
		if pageSize < 0 {
			return 0, ErrNegativePageSize
		}

		if pageSize > MaxPageSize {
			pageSize = MaxPageSize
		}
	} else {
		pageSize = DefaultPageSize
	}
	return uint(pageSize), nil
}
