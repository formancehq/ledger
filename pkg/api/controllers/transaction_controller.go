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
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
)

type TransactionController struct{}

func NewTransactionController() TransactionController {
	return TransactionController{}
}

func (ctl *TransactionController) CountTransactions(c *gin.Context) {
	l, _ := c.Get("ledger")

	var startTimeParsed, endTimeParsed time.Time
	var err error
	if c.Query("start_time") != "" {
		startTimeParsed, err = time.Parse(time.RFC3339, c.Query("start_time"))
		if err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError("invalid 'start_time' query param"))
			return
		}
	}

	if c.Query("end_time") != "" {
		endTimeParsed, err = time.Parse(time.RFC3339, c.Query("end_time"))
		if err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError("invalid 'end_time' query param"))
			return
		}
	}

	txQuery := ledger.NewTransactionsQuery().
		WithReferenceFilter(c.Query("reference")).
		WithAccountFilter(c.Query("account")).
		WithSourceFilter(c.Query("source")).
		WithDestinationFilter(c.Query("destination")).
		WithStartTimeFilter(startTimeParsed).
		WithEndTimeFilter(endTimeParsed)

	count, err := l.(*ledger.Ledger).CountTransactions(
		c.Request.Context(),
		*txQuery,
	)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	c.Header("Count", fmt.Sprint(count))
}

func (ctl *TransactionController) GetTransactions(c *gin.Context) {
	l, _ := c.Get("ledger")

	txQuery := ledger.NewTransactionsQuery()

	if c.Query(QueryKeyCursor) != "" {
		if c.Query("after") != "" || c.Query("reference") != "" ||
			c.Query("account") != "" || c.Query("source") != "" ||
			c.Query("destination") != "" || c.Query("start_time") != "" ||
			c.Query("end_time") != "" || c.Query("page_size") != "" {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("no other query params can be set with '%s'", QueryKeyCursor)))
			return
		}

		res, err := base64.RawURLEncoding.DecodeString(c.Query(QueryKeyCursor))
		if err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

		token := sqlstorage.TxsPaginationToken{}
		if err = json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

		txQuery = txQuery.
			WithAfterTxID(token.AfterTxID).
			WithReferenceFilter(token.ReferenceFilter).
			WithAccountFilter(token.AccountFilter).
			WithSourceFilter(token.SourceFilter).
			WithDestinationFilter(token.DestinationFilter).
			WithStartTimeFilter(token.StartTime).
			WithEndTimeFilter(token.EndTime).
			WithMetadataFilter(token.MetadataFilter).
			WithPageSize(token.PageSize)

	} else if c.Query(QueryKeyCursorDeprecated) != "" {
		if c.Query("after") != "" || c.Query("reference") != "" ||
			c.Query("account") != "" || c.Query("source") != "" ||
			c.Query("destination") != "" || c.Query("start_time") != "" ||
			c.Query("end_time") != "" || c.Query("page_size") != "" {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("no other query params can be set with '%s'", QueryKeyCursorDeprecated)))
			return
		}

		res, err := base64.RawURLEncoding.DecodeString(c.Query(QueryKeyCursorDeprecated))
		if err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursorDeprecated)))
			return
		}

		token := sqlstorage.TxsPaginationToken{}
		if err = json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursorDeprecated)))
			return
		}

		txQuery = txQuery.
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
		var err error
		var afterTxIDParsed uint64
		if c.Query("after") != "" {
			afterTxIDParsed, err = strconv.ParseUint(c.Query("after"), 10, 64)
			if err != nil {
				apierrors.ResponseError(c, ledger.NewValidationError(
					"invalid 'after' query param"))
				return
			}
		}

		var startTimeParsed, endTimeParsed time.Time
		if c.Query("start_time") != "" {
			startTimeParsed, err = time.Parse(time.RFC3339, c.Query("start_time"))
			if err != nil {
				apierrors.ResponseError(c, ledger.NewValidationError(
					"invalid 'start_time' query param"))
				return
			}
		}

		if c.Query("end_time") != "" {
			endTimeParsed, err = time.Parse(time.RFC3339, c.Query("end_time"))
			if err != nil {
				apierrors.ResponseError(c, ledger.NewValidationError(
					"invalid 'end_time' query param"))
				return
			}
		}

		pageSize, err := getPageSize(c)
		if err != nil {
			apierrors.ResponseError(c, err)
			return
		}

		txQuery = txQuery.
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

	cursor, err := l.(*ledger.Ledger).GetTransactions(c.Request.Context(), *txQuery)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithCursor[core.ExpandedTransaction](c, http.StatusOK, cursor)
}

type PostTransaction struct {
	Postings  core.Postings `json:"postings"`
	Script    core.Script   `json:"script"`
	Timestamp time.Time     `json:"timestamp"`
	Reference string        `json:"reference"`
	Metadata  core.Metadata `json:"metadata" swaggertype:"object"`
}

func (ctl *TransactionController) PostTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	value, ok := c.GetQuery("preview")
	preview := ok &&
		(strings.ToUpper(value) == "YES" || strings.ToUpper(value) == "TRUE" || value == "1")

	payload := PostTransaction{}
	if err := c.ShouldBindJSON(&payload); err != nil {
		apierrors.ResponseError(c,
			ledger.NewValidationError("invalid transaction format"))
		return
	}

	script := core.ScriptData{
		Script:    payload.Script,
		Timestamp: payload.Timestamp,
		Reference: payload.Reference,
		Metadata:  payload.Metadata,
	}

	if len(payload.Postings) > 0 {
		txData := core.TransactionData{
			Postings:  payload.Postings,
			Timestamp: payload.Timestamp,
			Reference: payload.Reference,
			Metadata:  payload.Metadata,
		}
		i, err := l.(*ledger.Ledger).ValidatePostings(c.Request.Context(), txData)
		if err != nil {
			apierrors.ResponseError(c, ledger.NewTransactionCommitError(i, err))
			return
		}
		postingsScript := core.TxsToScriptsData(txData)[0]
		script.Plain = postingsScript.Plain + script.Plain
	}

	res, err := l.(*ledger.Ledger).Execute(c.Request.Context(), preview, script)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithData[[]core.ExpandedTransaction](c, http.StatusOK, res)
}

func (ctl *TransactionController) GetTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	txId, err := strconv.ParseUint(c.Param("txid"), 10, 64)
	if err != nil {
		apierrors.ResponseError(c, ledger.NewValidationError("invalid transaction ID"))
		return
	}

	tx, err := l.(*ledger.Ledger).GetTransaction(c.Request.Context(), txId)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithData[*core.ExpandedTransaction](c, http.StatusOK, tx)
}

func (ctl *TransactionController) RevertTransaction(c *gin.Context) {
	l, _ := c.Get("ledger")

	txId, err := strconv.ParseUint(c.Param("txid"), 10, 64)
	if err != nil {
		apierrors.ResponseError(c, ledger.NewValidationError("invalid transaction ID"))
		return
	}

	tx, err := l.(*ledger.Ledger).RevertTransaction(c.Request.Context(), txId)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithData[*core.ExpandedTransaction](c, http.StatusOK, tx)
}

func (ctl *TransactionController) PostTransactionMetadata(c *gin.Context) {
	l, _ := c.Get("ledger")

	var m core.Metadata
	if err := c.ShouldBindJSON(&m); err != nil {
		apierrors.ResponseError(c, ledger.NewValidationError("invalid metadata format"))
		return
	}

	txId, err := strconv.ParseUint(c.Param("txid"), 10, 64)
	if err != nil {
		apierrors.ResponseError(c, ledger.NewValidationError("invalid transaction ID"))
		return
	}

	_, err = l.(*ledger.Ledger).GetTransaction(c.Request.Context(), txId)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	if err := l.(*ledger.Ledger).SaveMeta(c.Request.Context(),
		core.MetaTargetTypeTransaction, txId, m); err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithNoContent(c)
}

func (ctl *TransactionController) PostTransactionsBatch(c *gin.Context) {
	l, _ := c.Get("ledger")

	var txs core.Transactions
	if err := c.ShouldBindJSON(&txs); err != nil {
		apierrors.ResponseError(c, ledger.NewValidationError("invalid transactions format"))
		return
	}

	if len(txs.Transactions) == 0 {
		apierrors.ResponseError(c, ledger.NewValidationError("no transaction to insert"))
		return
	}

	i, err := l.(*ledger.Ledger).ValidatePostings(c.Request.Context(), txs.Transactions...)
	if err != nil {
		apierrors.ResponseError(c, ledger.NewTransactionCommitError(i, err))
		return
	}

	res, err := l.(*ledger.Ledger).Execute(c.Request.Context(), false,
		core.TxsToScriptsData(txs.Transactions...)...)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithData[[]core.ExpandedTransaction](c, http.StatusOK, res)
}
