package controllers

import (
	"encoding/base64"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"net/http"
)

type BalanceController struct{}

func NewBalanceController() BalanceController {
	return BalanceController{}
}

func (ctl *BalanceController) GetBalancesAggregated(c *gin.Context) {
	l, _ := c.Get("ledger")

	// if there was a need to do params validation, it would be done here.
	// With a c.GetQuery for required params
	// other job focused validation, like checking if a parameter is a number should also be done here

	var balances core.Balances
	var err error

	balancesQuery := storage.NewBalancesQuery(
		0,
		0,
		"", // no after needed in this endpoint
		&storage.BalancesQueryFilters{
			Address: c.Query("address"),
		})

	balances, err = l.(*ledger.Ledger).GetBalancesAggregated(
		c.Request.Context(),
		balancesQuery,
	)

	if err != nil {
		ResponseError(c, err)
		return
	}

	respondWithData[core.Balances](c, http.StatusOK, balances)
}

func (ctl *BalanceController) GetBalances(c *gin.Context) {
	l, _ := c.Get("ledger")

	var cursor sharedapi.Cursor[core.AccountsBalances]
	var balancesQuery storage.BalancesQuery
	var err error

	if c.Query("pagination_token") != "" {
		if c.Query("after") != "" ||
			c.Query("address") != "" {
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
		if err = json.Unmarshal(res, &token); err != nil {
			ResponseError(c, ledger.NewValidationError(
				"invalid query value 'pagination_token'"))
			return
		}

		balancesQuery = storage.NewBalancesQuery(
			token.Offset,
			0,
			token.AfterAddress,
			&storage.BalancesQueryFilters{
				Address: token.AddressRegexpFilter,
			})

	} else {
		balancesQuery = storage.NewBalancesQuery(
			0,
			0,
			c.Query("after"),
			&storage.BalancesQueryFilters{
				Address: c.Query("address"),
			})
	}

	cursor, err = l.(*ledger.Ledger).GetBalances(c.Request.Context(), balancesQuery)

	if err != nil {
		ResponseError(c, err)
		return
	}

	respondWithCursor[core.AccountsBalances](c, http.StatusOK, cursor)
}
