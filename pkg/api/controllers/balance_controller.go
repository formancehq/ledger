package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/api/struct_api"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"net/http"
)

type BalanceController struct{}

func NewBalanceController() BalanceController {
	return BalanceController{}
}

func (ctl *BalanceController) GetBalances(c *gin.Context) {
	l, _ := c.Get("ledger")

	// if there was a need to do params validation, it would be done here.
	// With a c.GetQuery for required params
	// other job focused validation, like checking if a parameter is a number should also be done here

	var cursor sharedapi.Cursor[core.AggregatedBalances]
	var err error

	cursor, err = l.(*ledger.Ledger).GetAggregatedBalances(
		c.Request.Context(),
		storage.BalancesQuery{
			AfterAddress: c.Query("after"),
			Params: struct_api.GetBalancesStruct{
				Account: c.Query("account"),
			},
		},
	)

	if err != nil {
		ResponseError(c, err)
		return
	}

	respondWithCursor[core.AggregatedBalances](c, http.StatusOK, cursor)
}
