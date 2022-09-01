package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/errors"
	"github.com/numary/ledger/pkg/ledger"
)

type LedgerController struct{}

func NewLedgerController() LedgerController {
	return LedgerController{}
}

func (ctl *LedgerController) GetStats(c *gin.Context) {
	l, _ := c.Get("ledger")

	stats, err := l.(*ledger.Ledger).Stats(c.Request.Context())
	if err != nil {
		errors.ResponseError(c, err)
		return
	}
	respondWithData[ledger.Stats](c, http.StatusOK, stats)
}
