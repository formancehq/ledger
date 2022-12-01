package controllers

import (
	"net/http"

	"github.com/formancehq/go-libs/sharedapi"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/core"
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
		apierrors.ResponseError(c, err)
		return
	}
	respondWithData[ledger.Stats](c, http.StatusOK, stats)
}

func (ctl *LedgerController) GetLogs(c *gin.Context) {
	l, _ := c.Get("ledger")

	var cursor sharedapi.Cursor[core.Log]
	_, err := l.(*ledger.Ledger).Logs(c.Request.Context())
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithCursor[core.Log](c, http.StatusOK, cursor)
}
