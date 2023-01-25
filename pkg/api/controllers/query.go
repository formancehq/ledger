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

	QueryKeyPageSize = "pageSize"
	// Deprecated
	QueryKeyPageSizeDeprecated = "page_size"

	QueryKeyBalanceOperator = "balanceOperator"
	// Deprecated
	QueryKeyBalanceOperatorDeprecated = "balance_operator"

	QueryKeyStartTime = "startTime"
	// Deprecated
	QueryKeyStartTimeDeprecated = "start_time"

	QueryKeyEndTime = "endTime"
	// Deprecated
	QueryKeyEndTimeDeprecated = "end_time"
)

var (
	ErrInvalidPageSize = ledger.NewValidationError("invalid 'pageSize' query param")
	// Deprecated
	ErrInvalidPageSizeDeprecated = ledger.NewValidationError("invalid 'page_size' query param")

	ErrInvalidBalanceOperator = ledger.NewValidationError(
		"invalid parameter 'balanceOperator', should be one of 'e, ne, gt, gte, lt, lte'")
	// Deprecated
	ErrInvalidBalanceOperatorDeprecated = ledger.NewValidationError(
		"invalid parameter 'balance_operator', should be one of 'e, ne, gt, gte, lt, lte'")

	ErrInvalidStartTime = ledger.NewValidationError("invalid 'startTime' query param")
	// Deprecated
	ErrInvalidStartTimeDeprecated = ledger.NewValidationError("invalid 'start_time' query param")

	ErrInvalidEndTime = ledger.NewValidationError("invalid 'endTime' query param")
	// Deprecated
	ErrInvalidEndTimeDeprecated = ledger.NewValidationError("invalid 'end_time' query param")
)

func getPageSize(c *gin.Context) (uint, error) {
	pageSizeParam := c.Query(QueryKeyPageSize)
	pageSizeParamDeprecated := c.Query(QueryKeyPageSizeDeprecated)
	if pageSizeParam == "" && pageSizeParamDeprecated == "" {
		return DefaultPageSize, nil
	}

	var pageSize uint64
	var err error
	if pageSizeParam != "" {
		pageSize, err = strconv.ParseUint(pageSizeParam, 10, 32)
		if err != nil {
			return 0, ErrInvalidPageSize
		}
	} else if pageSizeParamDeprecated != "" {
		pageSize, err = strconv.ParseUint(pageSizeParamDeprecated, 10, 32)
		if err != nil {
			return 0, ErrInvalidPageSizeDeprecated
		}
	}

	if pageSize > MaxPageSize {
		return MaxPageSize, nil
	}

	return uint(pageSize), nil
}

func getBalanceOperator(c *gin.Context) (ledger.BalanceOperator, error) {
	balanceOperator := ledger.DefaultBalanceOperator
	balanceOperatorStr := c.Query(QueryKeyBalanceOperator)
	balanceOperatorStrDeprecated := c.Query(QueryKeyBalanceOperatorDeprecated)
	if balanceOperatorStr != "" {
		var ok bool
		if balanceOperator, ok = ledger.NewBalanceOperator(balanceOperatorStr); !ok {
			return "", ErrInvalidBalanceOperator
		}
	} else if balanceOperatorStrDeprecated != "" {
		var ok bool
		if balanceOperator, ok = ledger.NewBalanceOperator(balanceOperatorStrDeprecated); !ok {
			return "", ErrInvalidBalanceOperatorDeprecated
		}
	}

	return balanceOperator, nil
}
