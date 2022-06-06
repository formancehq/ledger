package controllers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledger/query"
)

type TransactionController struct {
	BaseController
}

func NewTransactionController() TransactionController {
	return TransactionController{}
}

func (ctl *TransactionController) CountTransactions(c *gin.Context) {
	l, _ := c.Get("ledger")

	count, err := l.(*ledger.Ledger).CountTransactions(
		c.Request.Context(),
		query.Reference(c.Query("reference")),
		query.Account(c.Query("account")),
		query.Source(c.Query("source")),
		query.Destination(c.Query("destination")),
	)
	if err != nil {
		ResponseError(c, err)
		return
	}

	c.Header("Count", fmt.Sprint(count))
}

func (ctl *TransactionController) GetTransactions(c *gin.Context) {
	l, _ := c.Get("ledger")

	var startTime, endTime time.Time
	var err error
	if c.Query("start_time") != "" {
		startTime, err = time.Parse(time.RFC3339, c.Query("start_time"))
		if err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'start_time'"))
			return
		}
	}

	if c.Query("end_time") != "" {
		endTime, err = time.Parse(time.RFC3339, c.Query("end_time"))
		if err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'end_time'"))
			return
		}
	}

	after := c.Query("after")
	if c.Query("pagination_token") != "" {
		res, err := base64.RawURLEncoding.DecodeString(c.Query("pagination_token"))
		if err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}
		t := TransactionsPaginationToken{}
		if err = json.Unmarshal(res, &t); err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}
		after = strconv.FormatUint(t.ID, 10)
	}

	cursor, err := l.(*ledger.Ledger).GetTransactions(
		c.Request.Context(),
		query.After(after),
		query.Reference(c.Query("reference")),
		query.Account(c.Query("account")),
		query.Source(c.Query("source")),
		query.Destination(c.Query("destination")),
		query.StartTime(startTime),
		query.EndTime(endTime),
	)
	if err != nil {
		ResponseError(c, err)
		return
	}

	ctl.response(c, http.StatusOK, cursor)
}

type TransactionsPaginationToken struct {
	ID uint64 `json:"txid"`
}

func TransactionsTokenMarshal(i interface{}) (string, error) {
	raw, err := json.Marshal(i)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func (ctl *TransactionController) PostTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	value, ok := c.GetQuery("preview")
	preview := ok && (strings.ToUpper(value) == "YES" || strings.ToUpper(value) == "TRUE" || value == "1")

	var t core.TransactionData
	if err := c.ShouldBindJSON(&t); err != nil {
		panic(err)
	}

	fn := l.(*ledger.Ledger).Commit
	if preview {
		fn = l.(*ledger.Ledger).CommitPreview
	}

	_, txs, err := fn(c.Request.Context(), []core.TransactionData{t})
	if err != nil {
		ResponseError(c, err)
		return
	}

	if preview {
		ctl.response(c, http.StatusNotModified, txs)
	} else {
		ctl.response(c, http.StatusOK, txs)
	}
}

func (ctl *TransactionController) GetTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	txId, err := strconv.ParseUint(c.Param("txid"), 10, 64)
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	tx, err := l.(*ledger.Ledger).GetTransaction(c.Request.Context(), txId)
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

	txId, err := strconv.ParseUint(c.Param("txid"), 10, 64)
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	tx, err := l.(*ledger.Ledger).RevertTransaction(c.Request.Context(), txId)
	if err != nil {
		ResponseError(c, err)
		return
	}

	ctl.response(c, http.StatusOK, tx)
}

func (ctl *TransactionController) PostTransactionMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")

	var m core.Metadata
	if err := c.ShouldBindJSON(&m); err != nil {
		panic(err)
	}

	txId, err := strconv.ParseUint(c.Param("txid"), 10, 64)
	if err != nil {
		c.AbortWithStatus(http.StatusBadRequest)
		return
	}

	if err := l.(*ledger.Ledger).SaveMeta(c.Request.Context(), core.MetaTargetTypeTransaction, txId, m); err != nil {
		ResponseError(c, err)
		return
	}

	ctl.noContent(c)
}

func (ctl *TransactionController) PostTransactionsBatch(c *gin.Context) {
	l, _ := c.Get("ledger")

	var t core.Transactions
	if err := c.ShouldBindJSON(&t); err != nil {
		ResponseError(c, err)
		return
	}

	_, txs, err := l.(*ledger.Ledger).Commit(c.Request.Context(), t.Transactions)
	if err != nil {
		ResponseError(c, err)
		return
	}

	ctl.response(c, http.StatusOK, txs)
}
