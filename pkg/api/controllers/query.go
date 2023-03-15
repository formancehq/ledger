package controllers

import (
	"net/http"
	"strconv"

	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
)

const (
	MaxPageSize     = 1000
	DefaultPageSize = storage.QueryDefaultPageSize

	QueryKeyCursor          = "cursor"
	QueryKeyPageSize        = "pageSize"
	QueryKeyBalanceOperator = "balanceOperator"
	QueryKeyStartTime       = "startTime"
	QueryKeyEndTime         = "endTime"
)

var (
	ErrInvalidPageSize        = ledger.NewValidationError("invalid 'pageSize' query param")
	ErrInvalidBalanceOperator = ledger.NewValidationError(
		"invalid parameter 'balanceOperator', should be one of 'e, ne, gt, gte, lt, lte'")
	ErrInvalidStartTime = ledger.NewValidationError("invalid 'startTime' query param")
	ErrInvalidEndTime   = ledger.NewValidationError("invalid 'endTime' query param")
)

func getPageSize(w http.ResponseWriter, r *http.Request) (uint, error) {
	pageSizeParam := r.URL.Query().Get(QueryKeyPageSize)
	if pageSizeParam == "" {
		return DefaultPageSize, nil
	}

	var pageSize uint64
	var err error
	if pageSizeParam != "" {
		pageSize, err = strconv.ParseUint(pageSizeParam, 10, 32)
		if err != nil {
			return 0, ErrInvalidPageSize
		}
	}

	if pageSize > MaxPageSize {
		return MaxPageSize, nil
	}

	return uint(pageSize), nil
}

func getBalanceOperator(w http.ResponseWriter, r *http.Request) (storage.BalanceOperator, error) {
	balanceOperator := storage.DefaultBalanceOperator
	balanceOperatorStr := r.URL.Query().Get(QueryKeyBalanceOperator)
	if balanceOperatorStr != "" {
		var ok bool
		if balanceOperator, ok = storage.NewBalanceOperator(balanceOperatorStr); !ok {
			return "", ErrInvalidBalanceOperator
		}
	}

	return balanceOperator, nil
}
