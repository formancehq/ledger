package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/ledger"
)

// LedgerController -
type LedgerController struct {
	BaseController
}

// NewLedgerController -
func NewLedgerController() LedgerController {
	return LedgerController{}
}

// GetStats -
func (ctl *LedgerController) GetStats(c *gin.Context) {
	l, _ := c.Get("ledger")

	stats, err := l.(*ledger.Ledger).Stats()
	if err != nil {
		ctl.responseError(
			c,
			http.StatusInternalServerError,
			err,
		)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		stats,
	)
}
