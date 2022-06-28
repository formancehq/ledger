package controllers

import (
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
)

const (
	MaxLimit     = 1000
	DefaultLimit = storage.QueryDefaultLimit
)

var (
	ErrInvalidLimit  = ledger.NewValidationError("invalid query value 'limit'")
	ErrNegativeLimit = ledger.NewValidationError("cannot pass negative 'limit'")
)

func getLimit(c *gin.Context) (uint, error) {
	var (
		// Use int instead of uint because if the client pass a negative limit
		// the uint will transparently convert the value to a valid uint
		// and the error will not be catched
		limit int64
		err   error
	)
	if limitParam := c.Query("limit"); limitParam != "" {
		limit, err = strconv.ParseInt(limitParam, 10, 64)
		if err != nil {
			return 0, ErrInvalidLimit
		}
		if limit < 0 {
			return 0, ErrNegativeLimit
		}

		if limit > MaxLimit {
			limit = MaxLimit
		}
	} else {
		limit = DefaultLimit
	}
	return uint(limit), nil
}
