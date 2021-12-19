package controllers

import (
	"errors"
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
// @Description Get all ledger transactions
// @Tags transactions
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
// @Summary Create Transaction
// @Description Create a new ledger transaction
// @Tags transactions
// @Schemes
// @Description Commit a new transaction to the ledger
// @Param ledger path string true "ledger"
// @Param transaction body core.Transaction true "transaction"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Failure 400
// @Router /{ledger}/transactions [post]
func (ctl *TransactionController) PostTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	var t core.Transaction
	if err := c.ShouldBindJSON(&t); err != nil {
		ctl.responseError(
			c,
			http.StatusBadRequest,
			err,
		)
		return
	}

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

// PostTransactionsBatch godoc
// @Summary Create Transactions Batch
// @Description Create a new ledger transactions batch
// @Tags transactions
// @Schemes
// @Description Commit a batch of new transactions to the ledger
// @Param ledger path string true "ledger"
// @Param transactions body core.Transactions true "transactions"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Failure 400
// @Router /{ledger}/transactions/batch [post]
func (ctl *TransactionController) PostTransactionsBatch(c *gin.Context) {
	l, _ := c.Get("ledger")

	var transactions core.Transactions
	if err := c.ShouldBindJSON(&transactions); err != nil {
		ctl.responseError(
			c,
			http.StatusBadRequest,
			err,
		)
		return
	}

	var ts [][]core.Transaction
	for _, t := range transactions.Transactions {
		tx, err := l.(*ledger.Ledger).Commit([]core.Transaction{t})
		if err != nil {
			ctl.responseError(
				c,
				http.StatusInternalServerError,
				err,
			)
			return
		}
		ts = append(ts, tx)
	}

	ctl.response(
		c,
		http.StatusOK,
		ts,
	)
}

// GetTransaction godoc
// @Summary Revert Transaction
// @Description Get transaction by transaction id
// @Tags transactions
// @Schemes
// @Param ledger path string true "ledger"
// @Param txid path string true "txid"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Failure 404 {object} controllers.BaseResponse
// @Router /{ledger}/transactions/{txid} [get]
func (ctl *TransactionController) GetTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")
	tx, err := l.(*ledger.Ledger).GetTransaction(c.Param("txid"))
	if err != nil {
		ctl.responseError(
			c,
			http.StatusInternalServerError,
			err,
		)
		return
	}
	if tx.Postings == nil {
		ctl.responseError(
			c,
			http.StatusNotFound,
			errors.New("transaction not found"),
		)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		tx,
	)
}

// RevertTransaction godoc
// @Summary Revert Transaction
// @Description Revert a ledger transaction by transaction id
// @Tags transactions
// @Schemes
// @Param ledger path string true "ledger"
// @Param txid path string true "txid"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Router /{ledger}/transactions/{txid}/revert [post]
func (ctl *TransactionController) RevertTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")
	err := l.(*ledger.Ledger).RevertTransaction(c.Param("txid"))
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
// @Summary Set Transaction Metadata
// @Description Set a new metadata to a ledger transaction by transaction id
// @Tags transactions
// @Schemes
// @Param ledger path string true "ledger"
// @Param txid path string true "txid"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse
// @Router /{ledger}/transactions/{txid}/metadata [post]
func (ctl *TransactionController) PostTransactionMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")

	var m core.Metadata
	c.ShouldBind(&m)

	err := l.(*ledger.Ledger).SaveMeta(
		"transaction",
		c.Param("txid"),
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
