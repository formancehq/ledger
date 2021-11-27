package controllers

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger"
	"github.com/numary/ledger/ledger/query"
)

// TransactionController -
type TransactionController struct {
	BaseController
}

// NewTransactionController -
func NewTransactionController() TransactionController {
	return TransactionController{}
}

// GetTransactions godoc
// @Summary Get Transactions
// @Schemes
// @Description List transactions
// @Param ledger path string true "ledger"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse{cursor=query.Cursor{data=[]core.Transaction}}
// @Router /{ledger}/transactions [get]
func (ctl *TransactionController) GetTransactions(c *gin.Context) {
	l, _ := c.Get("ledger")
	cursor, err := l.(*ledger.Ledger).FindTransactions(
		query.After(c.Query("after")),
		query.Reference(c.Query("reference")),
		query.Account(c.Query("account")),
		query.Metakey(c.Query("meta_key")),
		query.Metavalue(c.Query("meta_value")),
	)
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
		cursor,
	)
}

// PostTransactions godoc
// @Summary Commit a new transaction to the ledger
// @Schemes
// @Description Commit a new transaction to the ledger
// @Param ledger path string true "ledger"
// @Param transaction body core.Transaction true "transaction"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Router /{ledger}/transactions [post]
func (ctl *TransactionController) PostTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	var t core.Transaction
	c.ShouldBind(&t)

	ts, err := l.(*ledger.Ledger).Commit([]core.Transaction{t})
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
		ts,
	)
}

// RevertTransaction godoc
// @Summary Revert transaction
// @Schemes
// @Param ledger path string true "ledger"
// @Param transactionId path string true "transactionId"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Router /{ledger}/transactions/{transactionId}/revert [post]
func (ctl *TransactionController) RevertTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")
	err := l.(*ledger.Ledger).RevertTransaction(c.Param("transactionId"))
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
		nil,
	)
}

// PostTransactionMetadata godoc
// @Summary Set metadata on transaction
// @Schemes
// @Param ledger path string true "ledger"
// @Param reference path string true "reference"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Router /{ledger}/transactions/{reference}/metadata [post]
func (ctl *TransactionController) PostTransactionMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")

	var m core.Metadata
	c.ShouldBind(&m)

	err := l.(*ledger.Ledger).SaveMeta(
		"transaction",
		c.Param("transactionId"),
		m,
	)
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
		nil,
	)
}
