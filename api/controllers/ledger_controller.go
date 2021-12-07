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

// GetStats godoc
// @Summary Get Stats
// @Description Get ledger stats (aggregate metrics on accounts and transactions)
// @Tags stats
// @Schemes
// @Description The stats for account
// @Accept json
// @Produce json
// @Param ledger path string true "ledger"
// @Success 200 {object} ledger.Stats{}
// @Router /{ledger}/stats [get]
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
