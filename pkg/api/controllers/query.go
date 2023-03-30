package controllers

import (
	"net/http"
	"strconv"

	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/pkg/errors"
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
	ErrInvalidPageSize        = errors.New("invalid 'pageSize' query param")
	ErrInvalidBalanceOperator = errors.New(
		"invalid parameter 'balanceOperator', should be one of 'e, ne, gt, gte, lt, lte'")
	ErrInvalidStartTime = errors.New("invalid 'startTime' query param")
	ErrInvalidEndTime   = errors.New("invalid 'endTime' query param")
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
			return 0, errorsutil.NewError(ledger.ErrValidation, ErrInvalidPageSize)
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
			return "", errorsutil.NewError(ledger.ErrValidation, ErrInvalidBalanceOperator)
		}
	}

	return balanceOperator, nil
}
