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
	"github.com/pkg/errors"
)

type TransactionController struct{}

func NewTransactionController() TransactionController {
	return TransactionController{}
}

func (ctl *TransactionController) CountTransactions(c *gin.Context) {
	l, _ := c.Get("ledger")

	var startTimeParsed, endTimeParsed time.Time
	var err error
	if c.Query(QueryKeyStartTime) != "" {
		startTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyStartTime))
		if err != nil {
			apierrors.ResponseError(c, ErrInvalidStartTime)
			return
		}
	}
	if c.Query(QueryKeyStartTimeDeprecated) != "" {
		startTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyStartTimeDeprecated))
		if err != nil {
			apierrors.ResponseError(c, ErrInvalidStartTimeDeprecated)
			return
		}
	}

	if c.Query(QueryKeyEndTime) != "" {
		endTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyEndTime))
		if err != nil {
			apierrors.ResponseError(c, ErrInvalidEndTime)
			return
		}
	}
	if c.Query(QueryKeyEndTimeDeprecated) != "" {
		endTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyEndTimeDeprecated))
		if err != nil {
			apierrors.ResponseError(c, ErrInvalidEndTimeDeprecated)
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

	count, err := l.(*ledger.Ledger).CountTransactions(c.Request.Context(), *txQuery)
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
		if c.Query("after") != "" ||
			c.Query("reference") != "" ||
			c.Query("account") != "" ||
			c.Query("source") != "" ||
			c.Query("destination") != "" ||
			c.Query(QueryKeyStartTime) != "" ||
			c.Query(QueryKeyStartTimeDeprecated) != "" ||
			c.Query(QueryKeyEndTime) != "" ||
			c.Query(QueryKeyEndTimeDeprecated) != "" ||
			c.Query(QueryKeyPageSize) != "" ||
			c.Query(QueryKeyPageSizeDeprecated) != "" {
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
		if c.Query("after") != "" ||
			c.Query("reference") != "" ||
			c.Query("account") != "" ||
			c.Query("source") != "" ||
			c.Query("destination") != "" ||
			c.Query(QueryKeyStartTime) != "" ||
			c.Query(QueryKeyStartTimeDeprecated) != "" ||
			c.Query(QueryKeyEndTime) != "" ||
			c.Query(QueryKeyEndTimeDeprecated) != "" ||
			c.Query(QueryKeyPageSize) != "" ||
			c.Query(QueryKeyPageSizeDeprecated) != "" {
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
		if c.Query(QueryKeyStartTime) != "" {
			startTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyStartTime))
			if err != nil {
				apierrors.ResponseError(c, ErrInvalidStartTime)
				return
			}
		}
		if c.Query(QueryKeyStartTimeDeprecated) != "" {
			startTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyStartTimeDeprecated))
			if err != nil {
				apierrors.ResponseError(c, ErrInvalidStartTimeDeprecated)
				return
			}
		}

		if c.Query(QueryKeyEndTime) != "" {
			endTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyEndTime))
			if err != nil {
				apierrors.ResponseError(c, ErrInvalidEndTime)
				return
			}
		}
		if c.Query(QueryKeyEndTimeDeprecated) != "" {
			endTimeParsed, err = time.Parse(time.RFC3339, c.Query(QueryKeyEndTimeDeprecated))
			if err != nil {
				apierrors.ResponseError(c, ErrInvalidEndTimeDeprecated)
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

	var res []core.ExpandedTransaction
	var err error

	if len(payload.Postings) > 0 && payload.Script.Plain != "" ||
		len(payload.Postings) == 0 && payload.Script.Plain == "" {
		apierrors.ResponseError(c, ledger.NewValidationError(
			"invalid payload: should contain either postings or script"))
		return
	} else if len(payload.Postings) > 0 {
		if i, err := payload.Postings.Validate(); err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(errors.Wrap(err,
				fmt.Sprintf("invalid posting %d", i)).Error()))
			return
		}
		txData := core.TransactionData{
			Postings:  payload.Postings,
			Timestamp: payload.Timestamp,
			Reference: payload.Reference,
			Metadata:  payload.Metadata,
		}
		script := core.TxsToScriptsData(txData)
		res, err = l.(*ledger.Ledger).ExecuteScripts(c.Request.Context(), true, preview, script...)
	} else {
		script := core.ScriptData{
			Script:    payload.Script,
			Timestamp: payload.Timestamp,
			Reference: payload.Reference,
			Metadata:  payload.Metadata,
		}
		res, err = l.(*ledger.Ledger).ExecuteScripts(c.Request.Context(), false, preview, script)
	}
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

	for i, tx := range txs.Transactions {
		if len(tx.Postings) == 0 {
			apierrors.ResponseError(c, ledger.NewValidationError(errors.New(fmt.Sprintf(
				"invalid transaction %d: no postings", i)).Error()))
			return
		}
		if j, err := tx.Postings.Validate(); err != nil {
			apierrors.ResponseError(c, ledger.NewValidationError(errors.Wrap(err,
				fmt.Sprintf("invalid transaction %d: posting %d", i, j)).Error()))
			return
		}
	}

	res, err := l.(*ledger.Ledger).ExecuteScripts(c.Request.Context(), true, false,
		core.TxsToScriptsData(txs.Transactions...)...)
	if err != nil {
		apierrors.ResponseError(c, err)
		return
	}

	respondWithData[[]core.ExpandedTransaction](c, http.StatusOK, res)
}
