package controllers

import (
	"errors"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledger/query"
)

// TransactionController -
type TransactionController struct {
	BaseController
}

// NewTransactionController -
func NewTransactionController() TransactionController {
	return TransactionController{}
}

func (ctl *TransactionController) handleCommitError(c *gin.Context, err *ledger.TransactionCommitError) {
	switch err.Err.(type) {
	case *ledger.ConflictError:
		ctl.responseError(c, http.StatusConflict, ErrConflict, err)
	case *ledger.InsufficientFundError:
		ctl.responseError(c, http.StatusBadRequest, ErrInsufficientFund, err)
	case *ledger.ValidationError:
		ctl.responseError(c, http.StatusBadRequest, ErrValidation, err)
	default:
		ctl.responseError(c, http.StatusInternalServerError, ErrInternal, err)
	}
}

// GetTransactions godoc
// @Summary Get all Transactions
// @Description Get all ledger transactions
// @Tags transactions
// @Schemes
// @Param ledger path string true "ledger"
// @Param after query string false "pagination cursor, will return transactions after given txid (in descending order)"
// @Param reference query string false "find transactions by reference field"
// @Param account query string false "find transactions with postings involving given account, either as source or destination"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse{cursor=query.Cursor{data=[]core.Transaction}}
// @Router /{ledger}/transactions [get]
func (ctl *TransactionController) GetTransactions(c *gin.Context) {
	l, _ := c.Get("ledger")
	cursor, err := l.(*ledger.Ledger).FindTransactions(
		c.Request.Context(),
		query.After(c.Query("after")),
		query.Reference(c.Query("reference")),
		query.Account(c.Query("account")),
	)
	if err != nil {
		ctl.responseError(c, http.StatusInternalServerError, ErrInternal, err)
		return
	}
	ctl.response(
		c,
		http.StatusOK,
		cursor,
	)
}

// PostTransaction godoc
// @Summary Create Transaction
// @Description Create a new ledger transaction
// @Tags transactions
// @Schemes
// @Description Commit a new transaction to the ledger
// @Param ledger path string true "ledger"
// @Param transaction body core.TransactionData true "transaction"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse{data=core.Transaction[]}
// @Failure 400 {object} controllers.ErrorResponse
// @Failure 409 {object} controllers.ErrorResponse
// @Router /{ledger}/transactions [post]
func (ctl *TransactionController) PostTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	var t core.TransactionData
	c.ShouldBind(&t)

	_, result, err := l.(*ledger.Ledger).Commit(c.Request.Context(), []core.TransactionData{t})
	if err != nil {
		switch err {
		case ledger.ErrCommitError:
			tx := result[0]
			ctl.handleCommitError(c, tx.Err)
		default:
			ctl.responseError(c, http.StatusInternalServerError, ErrInternal, err)
		}
		return
	}
	ctl.response(c, http.StatusOK, result[0])
}

// GetTransaction godoc
// @Summary Get Transaction
// @Description Get transaction by transaction id
// @Tags transactions
// @Schemes
// @Param ledger path string true "ledger"
// @Param txid path string true "txid"
// @Accept json
// @Produce json
// @Success 200 {object} controllers.BaseResponse{data=core.Transaction}
// @Failure 404 {object} controllers.BaseResponse
// @Router /{ledger}/transactions/{txid} [get]
func (ctl *TransactionController) GetTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")
	tx, err := l.(*ledger.Ledger).GetTransaction(c.Request.Context(), c.Param("txid"))
	if err != nil {
		ctl.responseError(c, http.StatusInternalServerError, ErrInternal, err)
		return
	}
	if tx.Postings == nil {
		ctl.responseError(c, http.StatusNotFound, ErrNotFound, errors.New("transaction not found"))
		return
	}
	ctl.response(c, http.StatusOK, tx)
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
// @Success 204 "Empty response"
// @Router /{ledger}/transactions/{txid}/revert [post]
func (ctl *TransactionController) RevertTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")
	err := l.(*ledger.Ledger).RevertTransaction(c.Request.Context(), c.Param("txid"))
	if err != nil {
		switch ee := err.(type) {
		case *ledger.TransactionCommitError:
			ctl.handleCommitError(c, ee)
		default:
			ctl.responseError(c, http.StatusInternalServerError, ErrInternal, err)
		}
		return
	}
	ctl.noContent(c)
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
// @Success 204 "Empty response"
// @Router /{ledger}/transactions/{txid}/metadata [post]
func (ctl *TransactionController) PostTransactionMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")

	var m core.Metadata
	c.ShouldBind(&m)

	err := l.(*ledger.Ledger).SaveMeta(
		c.Request.Context(),
		"transaction",
		c.Param("txid"),
		m,
	)
	if err != nil {
		ctl.responseError(c, http.StatusInternalServerError, ErrInternal, err)
		return
	}
	ctl.noContent(c)
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
			ErrValidation,
			err,
		)
		return
	}

	_, ret, err := l.(*ledger.Ledger).Commit(c.Request.Context(), transactions.Transactions)
	if err != nil {
		ctl.responseError(
			c,
			http.StatusInternalServerError,
			ErrInternal,
			err,
		)
		return
	}

	ctl.response(
		c,
		http.StatusOK,
		ret,
	)
}
