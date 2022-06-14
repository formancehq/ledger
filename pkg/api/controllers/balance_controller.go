package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/struct_api"
	"github.com/numary/ledger/pkg/ledger"
	"net/http"
)

type BalanceController struct {
	BaseController
}

func NewBalanceController() BalanceController {
	return BalanceController{}
}

func (ctl *BalanceController) GetBalances(c *gin.Context) {
	l, _ := c.Get("ledger")

	// if there was a need to do params validation, it would be done here.
	// With a c.GetQuery for required params
	// other job focused validation, like checking if a parameter is a number should also be done here

	cursor, err := l.(*ledger.Ledger).GetAggregatedBalances(
		c.Request.Context(),
		struct_api.GetBalancesStruct{
			Account: c.Query("account"),
		},
	)
	if err != nil {
		ResponseError(c, err)
		return
	}

	ctl.response(c, http.StatusOK, cursor)
}
