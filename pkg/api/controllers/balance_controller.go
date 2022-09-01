package controllers

import (
	"encoding/base64"
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
)

type BalanceController struct{}

func NewBalanceController() BalanceController {
	return BalanceController{}
}

func (ctl *BalanceController) GetBalancesAggregated(c *gin.Context) {
	l, _ := c.Get("ledger")

	var balances core.AssetsBalances
	var err error

	balancesQuery := ledger.NewBalancesQuery().WithAddressFilter(c.Query("address"))

	balances, err = l.(*ledger.Ledger).GetBalancesAggregated(
		c.Request.Context(),
		*balancesQuery,
	)

	if err != nil {
		ResponseError(c, err)
		return
	}

	respondWithData[core.AssetsBalances](c, http.StatusOK, balances)
}

func (ctl *BalanceController) GetBalances(c *gin.Context) {
	l, _ := c.Get("ledger")

	balancesQuery := ledger.NewBalancesQuery()

	if c.Query("pagination_token") != "" {
		if c.Query("after") != "" ||
			c.Query("address") != "" ||
			c.Query("page_size") != "" {
			ResponseError(c, ledger.NewValidationError(
				"no other query params can be set with 'pagination_token'"))
			return
		}

		res, decErr := base64.RawURLEncoding.DecodeString(c.Query("pagination_token"))
		if decErr != nil {
			ResponseError(c, ledger.NewValidationError(
				"invalid query value 'pagination_token'"))
			return
		}

		token := sqlstorage.BalancesPaginationToken{}
		if err := json.Unmarshal(res, &token); err != nil {
			ResponseError(c, ledger.NewValidationError(
				"invalid query value 'pagination_token'"))
			return
		}

		balancesQuery = balancesQuery.
			WithOffset(token.Offset).
			WithAfterAddress(token.AfterAddress).
			WithAddressFilter(token.AddressRegexpFilter).
			WithPageSize(token.PageSize)

	} else {

		pageSize, err := getPageSize(c)
		if err != nil {
			ResponseError(c, err)
			return
		}

		balancesQuery = balancesQuery.
			WithAfterAddress(c.Query("after")).
			WithAddressFilter(c.Query("address")).
			WithPageSize(pageSize)
	}

	cursor, err := l.(*ledger.Ledger).GetBalances(c.Request.Context(), *balancesQuery)

	if err != nil {
		ResponseError(c, err)
		return
	}

	respondWithCursor[core.AccountsBalances](c, http.StatusOK, cursor)
}
