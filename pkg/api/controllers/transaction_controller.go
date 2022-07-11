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
	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
)

type TransactionController struct{}

func NewTransactionController() TransactionController {
	return TransactionController{}
}

func (ctl *TransactionController) CountTransactions(c *gin.Context) {
	l, _ := c.Get("ledger")

	txQuery := storage.NewTransactionsQuery().
		WithReferenceFilter(c.Query("reference")).
		WithAccountFilter(c.Query("account")).
		WithSourceFilter(c.Query("source")).
		WithDestinationFilter(c.Query("destination"))

	count, err := l.(*ledger.Ledger).CountTransactions(
		c.Request.Context(),
		*txQuery,
	)
	if err != nil {
		ResponseError(c, err)
		return
	}

	c.Header("Count", fmt.Sprint(count))
}

func (ctl *TransactionController) GetTransactions(c *gin.Context) {
	l, _ := c.Get("ledger")

	var cursor sharedapi.Cursor[core.Transaction]
	var txQuery *storage.TransactionsQuery
	var err error

	if c.Query("pagination_token") != "" {
		if c.Query("after") != "" || c.Query("reference") != "" ||
			c.Query("account") != "" || c.Query("source") != "" ||
			c.Query("destination") != "" || c.Query("start_time") != "" ||
			c.Query("end_time") != "" || c.Query("page_size") != "" {
			ResponseError(c, ledger.NewValidationError(
				"no other query params can be set with 'pagination_token'"))
			return
		}

		res, decErr := base64.RawURLEncoding.DecodeString(c.Query("pagination_token"))
		if decErr != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}

		token := sqlstorage.TxsPaginationToken{}
		if err = json.Unmarshal(res, &token); err != nil {
			ResponseError(c, ledger.NewValidationError("invalid query value 'pagination_token'"))
			return
		}

		txQuery = storage.NewTransactionsQuery().
			WithAfterTxID(token.AfterTxID).
			WithReferenceFilter(token.ReferenceFilter).
			WithAccountFilter(token.AccountFilter).
			WithSourceFilter(token.SourceFilter).
			WithDestinationFilter(token.DestinationFilter).
			WithStartTimeFilter(token.StartTime).
			WithEndTimeFilter(token.EndTime).
			WithMetadataFilter(token.MetadataFilter).
			WithPageSize(token.PageSize)
	} else {
		var afterTxIDParsed uint64
		if c.Query("after") != "" {
			afterTxIDParsed, err = strconv.ParseUint(c.Query("after"), 10, 64)
			if err != nil {
				ResponseError(c, ledger.NewValidationError("invalid query value 'after'"))
				return
			}
		}

		var startTimeParsed, endTimeParsed time.Time
		if c.Query("start_time") != "" {
			startTimeParsed, err = time.Parse(time.RFC3339, c.Query("start_time"))
			if err != nil {
				ResponseError(c, ledger.NewValidationError("invalid query value 'start_time'"))
				return
			}
		}

		if c.Query("end_time") != "" {
			endTimeParsed, err = time.Parse(time.RFC3339, c.Query("end_time"))
			if err != nil {
				ResponseError(c, ledger.NewValidationError("invalid query value 'end_time'"))
				return
			}
		}

		pageSize, err := getPageSize(c)
		if err != nil {
			ResponseError(c, err)
			return
		}

		txQuery = storage.NewTransactionsQuery().
			WithAfterTxID(afterTxIDParsed).
			WithReferenceFilter(c.Query("reference")).
			WithAccountFilter(c.Query("account")).
			WithSourceFilter(c.Query("source")).
			WithDestinationFilter(c.Query("destination")).
			WithStartTimeFilter(startTimeParsed).
			WithEndTimeFilter(endTimeParsed).
			WithMetadataFilter(c.QueryMap("metadata")).
			WithPageSize(pageSize)
	}

	cursor, err = l.(*ledger.Ledger).GetTransactions(c.Request.Context(), *txQuery)
	if err != nil {
		ResponseError(c, err)
		return
	}

	respondWithCursor[core.Transaction](c, http.StatusOK, cursor)
}

func (ctl *TransactionController) PostTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	value, ok := c.GetQuery("preview")
	preview := ok &&
		(strings.ToUpper(value) == "YES" || strings.ToUpper(value) == "TRUE" || value == "1")

	var txData core.TransactionData
	if err := c.ShouldBindJSON(&txData); err != nil {
		ResponseError(c, ledger.NewValidationError("invalid transaction format"))
		return
	}
	if len(txData.Postings) == 0 {
		ResponseError(c, ledger.NewValidationError("transaction has no postings"))
		return
	}

	fn := l.(*ledger.Ledger).Commit
	if preview {
		fn = l.(*ledger.Ledger).CommitPreview
	}

	res, err := fn(c.Request.Context(), []core.TransactionData{txData})
	if err != nil {
		ResponseError(c, err)
		return
	}

	status := http.StatusOK
	if preview {
		status = http.StatusNotModified
	}

	respondWithData[[]core.Transaction](c, status, res.GeneratedTransactions)
}

func (ctl *TransactionController) GetTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	txId, err := strconv.ParseUint(c.Param("txid"), 10, 64)
	if err != nil {
		ResponseError(c, ledger.NewValidationError("invalid transaction ID"))
		return
	}

	tx, err := l.(*ledger.Ledger).GetTransaction(c.Request.Context(), txId)
	if err != nil {
		ResponseError(c, err)
		return
	}

	respondWithData[*core.Transaction](c, http.StatusOK, tx)
}

func (ctl *TransactionController) RevertTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	txId, err := strconv.ParseUint(c.Param("txid"), 10, 64)
	if err != nil {
		ResponseError(c, ledger.NewValidationError("invalid transaction ID"))
		return
	}

	tx, err := l.(*ledger.Ledger).RevertTransaction(c.Request.Context(), txId)
	if err != nil {
		ResponseError(c, err)
		return
	}

	respondWithData[*core.Transaction](c, http.StatusOK, tx)
}

func (ctl *TransactionController) PostTransactionMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")

	var m core.Metadata
	if err := c.ShouldBindJSON(&m); err != nil {
		ResponseError(c, ledger.NewValidationError("invalid metadata format"))
		return
	}

	txId, err := strconv.ParseUint(c.Param("txid"), 10, 64)
	if err != nil {
		ResponseError(c, ledger.NewValidationError("invalid transaction ID"))
		return
	}

	_, err = l.(*ledger.Ledger).GetTransaction(c.Request.Context(), txId)
	if err != nil {
		ResponseError(c, err)
		return
	}

	if err := l.(*ledger.Ledger).SaveMeta(c.Request.Context(),
		core.MetaTargetTypeTransaction, txId, m); err != nil {
		ResponseError(c, err)
		return
	}

	respondWithNoContent(c)
}

func (ctl *TransactionController) PostTransactionsBatch(c *gin.Context) {
	l, _ := c.Get("ledger")

	var txs core.Transactions
	if err := c.ShouldBindJSON(&txs); err != nil {
		ResponseError(c, ledger.NewValidationError("invalid transactions format"))
		return
	}

	if len(txs.Transactions) == 0 {
		ResponseError(c, ledger.NewValidationError("no transaction to insert"))
		return
	}

	res, err := l.(*ledger.Ledger).Commit(c.Request.Context(), txs.Transactions)
	if err != nil {
		ResponseError(c, err)
		return
	}

	respondWithData[[]core.Transaction](c, http.StatusOK, res.GeneratedTransactions)
}
