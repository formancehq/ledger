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
	"github.com/numary/ledger/pkg/storage/sqlstorage"
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
		query.SetReferenceFilter(c.Query("reference")),
		query.SetAccountFilter(c.Query("account")),
		query.SetSourceFilter(c.Query("source")),
		query.SetDestinationFilter(c.Query("destination")),
	)
	if err != nil {
		ResponseError(c, err)
		return
	}

	c.Header("Count", fmt.Sprint(count))
}

func (ctl *TransactionController) GetTransactions(c *gin.Context) {
	l, _ := c.Get("ledger")
	paginationToken := c.Query("pagination_token")
	afterTxID := c.Query("after")
	referenceFilter := c.Query("reference")
	accountFilter := c.Query("account")
	sourceFilter := c.Query("source")
	destinationFilter := c.Query("destination")
	startTime := c.Query("start_time")
	endTime := c.Query("end_time")

	fmt.Printf("TOK:%s\n", paginationToken)
	if paginationToken != "" {
		if afterTxID != "" || referenceFilter != "" || accountFilter != "" ||
			sourceFilter != "" || destinationFilter != "" || startTime != "" || endTime != "" {
			ResponseError(c, ledger.NewValidationError(
				"no other query params can be set with 'pagination_token'"))
			return
		}
		res, err := base64.RawURLEncoding.DecodeString(paginationToken)
		if err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}
		t := sqlstorage.TxsPaginationToken{}
		if err = json.Unmarshal(res, &t); err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}

		cursor, err := l.(*ledger.Ledger).GetTransactions(
			c.Request.Context(),
			query.SetAfterTxID(t.AfterTxID),
			query.SetReferenceFilter(t.ReferenceFilter),
			query.SetAccountFilter(t.AccountFilter),
			query.SetSourceFilter(t.SourceFilter),
			query.SetDestinationFilter(t.DestinationFilter),
			query.SetStartTime(t.StartTime),
			query.SetEndTime(t.EndTime),
		)
		if err != nil {
			ResponseError(c, err)
		} else {
			ctl.response(c, http.StatusOK, cursor)
		}
		return
	}

	var afterTxIDParsed uint64
	var err error
	if afterTxID != "" {
		afterTxIDParsed, err = strconv.ParseUint(afterTxID, 10, 64)
		if err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'after'"))
			return
		}
	}

	var startTimeParsed, endTimeParsed time.Time
	if startTime != "" {
		startTimeParsed, err = time.Parse(time.RFC3339, startTime)
		if err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'start_time'"))
			return
		}
	}

	if endTime != "" {
		endTimeParsed, err = time.Parse(time.RFC3339, endTime)
		if err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'end_time'"))
			return
		}
	}

	cursor, err := l.(*ledger.Ledger).GetTransactions(
		c.Request.Context(),
		query.SetAfterTxID(afterTxIDParsed),
		query.SetReferenceFilter(referenceFilter),
		query.SetAccountFilter(accountFilter),
		query.SetSourceFilter(sourceFilter),
		query.SetDestinationFilter(destinationFilter),
		query.SetStartTime(startTimeParsed),
		query.SetEndTime(endTimeParsed),
	)
	if err != nil {
		ResponseError(c, err)
		return
	}

	ctl.response(c, http.StatusOK, cursor)
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
