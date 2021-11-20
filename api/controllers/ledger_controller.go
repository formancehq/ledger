package controllers

import (
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

	c.JSON(200, gin.H{
		"ok":    err == nil,
		"stats": stats,
	})
}
