package v1

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/formancehq/ledger/internal/storage/paginate"
)

const (
	MaxPageSize     = 1000
	DefaultPageSize = paginate.QueryDefaultPageSize

	QueryKeyCursor          = "cursor"
	QueryKeyPageSize        = "pageSize"
	QueryKeyBalanceOperator = "balanceOperator"
)

func getPageSize(c *http.Request) (uint, error) {
	pageSizeParam := c.URL.Query().Get(QueryKeyPageSize)
	if pageSizeParam == "" {
		return DefaultPageSize, nil
	}

	var pageSize uint64
	var err error
	if pageSizeParam != "" {
		pageSize, err = strconv.ParseUint(pageSizeParam, 10, 32)
		if err != nil {
			return 0, errors.New("invalid page size")
		}
	}

	if pageSize > MaxPageSize {
		return MaxPageSize, nil
	}

	return uint(pageSize), nil
}

func getBalanceOperator(c *http.Request) (string, error) {
	balanceOperator := "eq"
	balanceOperatorStr := c.URL.Query().Get(QueryKeyBalanceOperator)
	if balanceOperatorStr != "" {
		return balanceOperatorStr, nil
	}

	return balanceOperator, nil
}
