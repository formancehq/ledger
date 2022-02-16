package controllers

import (
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledger/query"
	"net/http"
	"strings"
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
		ResponseError(c, err)
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

	value, ok := c.GetQuery("preview")
	preview := ok && (strings.ToUpper(value) == "YES" || strings.ToUpper(value) == "TRUE" || value == "1")

	var t core.TransactionData
	c.ShouldBind(&t)

	fn := l.(*ledger.Ledger).Commit
	if preview {
		fn = l.(*ledger.Ledger).Preview
	}

	_, result, err := fn(c.Request.Context(), []core.TransactionData{t})
	if err != nil {
		switch err {
		case ledger.ErrCommitError:
			tx := result[0]
			ResponseError(c, tx.Err)
		default:
			ResponseError(c, err)
		}
		return
	}
	status := http.StatusOK
	if preview {
		status = http.StatusNotModified
	}
	ctl.response(c, status, result)
}

func (ctl *TransactionController) GetTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")
	tx, err := l.(*ledger.Ledger).GetTransaction(c.Request.Context(), c.Param("txid"))
	if err != nil {
		ResponseError(c, err)
		return
	}
	if len(tx.Postings) == 0 {
		c.AbortWithStatus(http.StatusNotFound)
		return
	}
	ctl.response(c, http.StatusOK, tx)
}

func (ctl *TransactionController) RevertTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")
	err := l.(*ledger.Ledger).RevertTransaction(c.Request.Context(), c.Param("txid"))
	if err != nil {
		ResponseError(c, err)
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
		ResponseError(c, err)
		return
	}
	ctl.noContent(c)
}

func (ctl *TransactionController) PostTransactionsBatch(c *gin.Context) {
	l, _ := c.Get("ledger")

	var transactions core.Transactions
	if err := c.ShouldBindJSON(&transactions); err != nil {
		ResponseError(c, err)
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
					var status int
					status, v.ErrorCode = coreErrorToErrorCode(tx.Err)
					if status < 500 {
						v.ErrorMessage = tx.Err.Error()
					}
				}
				results = append(results, v)
			}
			ctl.response(c, http.StatusBadRequest, results)
		default:
			ResponseError(c, err)
		}
		return
	}

	ctl.response(c, http.StatusOK, ret)
}
