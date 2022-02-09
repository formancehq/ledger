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

func (ctl *TransactionController) PostTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	var t core.TransactionData
	c.ShouldBind(&t)

	_, result, err := l.(*ledger.Ledger).Commit(c.Request.Context(), []core.TransactionData{t})
	if err != nil {
		switch err {
		case ledger.ErrCommitError:
			tx := result[0]
			ResponseError(c, tx.Err)
		default:
			ctl.responseError(c, http.StatusInternalServerError, ErrInternal, err)
		}
		return
	}
	ctl.response(c, http.StatusOK, result)
}

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

func (ctl *TransactionController) RevertTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")
	err := l.(*ledger.Ledger).RevertTransaction(c.Request.Context(), c.Param("txid"))
	if err != nil {
		switch ee := err.(type) {
		case *ledger.TransactionCommitError:
			ResponseError(c, ee)
		default:
			ctl.responseError(c, http.StatusInternalServerError, ErrInternal, err)
		}
		return
	}
	ctl.noContent(c)
}

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

func (ctl *TransactionController) PostTransactionsBatch(c *gin.Context) {
	l, _ := c.Get("ledger")

	var transactions core.Transactions
	if err := c.ShouldBindJSON(&transactions); err != nil {
		ctl.responseError(c, http.StatusBadRequest, ErrValidation, err)
		return
	}

	_, ret, err := l.(*ledger.Ledger).Commit(c.Request.Context(), transactions.Transactions)
	if err != nil {
		switch err {
		case ledger.ErrCommitError:
			type TransactionError struct {
				core.Transaction
				ErrorCode    string `json:"errorCode,omitempty"`
				ErrorMessage string `json:"errorMessage,omitempty"`
			}
			results := make([]TransactionError, 0)
			for _, tx := range ret {
				v := TransactionError{
					Transaction: tx.Transaction,
				}
				if tx.Err != nil {
					v.ErrorMessage = tx.Err.Error()
					_, v.ErrorCode = coreErrorToErrorCode(tx.Err)
				}
				results = append(results, v)
			}
			ctl.response(c, http.StatusBadRequest, results)
		default:
			ctl.responseError(c, http.StatusBadRequest, ErrInternal, err)
		}
		return
	}

	ctl.response(c, http.StatusOK, ret)
}
