package controllers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

func CountTransactions(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	var startTimeParsed, endTimeParsed core.Time
	var err error
	if r.URL.Query().Get(QueryKeyStartTime) != "" {
		startTimeParsed, err = core.ParseTime(r.URL.Query().Get(QueryKeyStartTime))
		if err != nil {
			apierrors.ResponseError(w, r, ErrInvalidStartTime)
			return
		}
	}

	if r.URL.Query().Get(QueryKeyEndTime) != "" {
		endTimeParsed, err = core.ParseTime(r.URL.Query().Get(QueryKeyEndTime))
		if err != nil {
			apierrors.ResponseError(w, r, ErrInvalidEndTime)
			return
		}
	}

	txQuery := storage.NewTransactionsQuery().
		WithReferenceFilter(r.URL.Query().Get("reference")).
		WithAccountFilter(r.URL.Query().Get("account")).
		WithSourceFilter(r.URL.Query().Get("source")).
		WithDestinationFilter(r.URL.Query().Get("destination")).
		WithStartTimeFilter(startTimeParsed).
		WithEndTimeFilter(endTimeParsed)

	count, err := l.CountTransactions(r.Context(), *txQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
}

func GetTransactions(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	txQuery := storage.NewTransactionsQuery()

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		if r.URL.Query().Get("after") != "" ||
			r.URL.Query().Get("reference") != "" ||
			r.URL.Query().Get("account") != "" ||
			r.URL.Query().Get("source") != "" ||
			r.URL.Query().Get("destination") != "" ||
			r.URL.Query().Get(QueryKeyStartTime) != "" ||
			r.URL.Query().Get(QueryKeyEndTime) != "" ||
			r.URL.Query().Get(QueryKeyPageSize) != "" {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("no other query params can be set with '%s'", QueryKeyCursor)))
			return
		}

		res, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get(QueryKeyCursor))
		if err != nil {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

		token := ledgerstore.TxsPaginationToken{}
		if err = json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
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

	} else {
		var err error
		var afterTxIDParsed uint64
		if r.URL.Query().Get("after") != "" {
			afterTxIDParsed, err = strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
			if err != nil {
				apierrors.ResponseError(w, r, ledger.NewValidationError(
					"invalid 'after' query param"))
				return
			}
		}

		var startTimeParsed, endTimeParsed core.Time
		if r.URL.Query().Get(QueryKeyStartTime) != "" {
			startTimeParsed, err = core.ParseTime(r.URL.Query().Get(QueryKeyStartTime))
			if err != nil {
				apierrors.ResponseError(w, r, ErrInvalidStartTime)
				return
			}
		}

		if r.URL.Query().Get(QueryKeyEndTime) != "" {
			endTimeParsed, err = core.ParseTime(r.URL.Query().Get(QueryKeyEndTime))
			if err != nil {
				apierrors.ResponseError(w, r, ErrInvalidEndTime)
				return
			}
		}

		pageSize, err := getPageSize(w, r)
		if err != nil {
			apierrors.ResponseError(w, r, err)
			return
		}

		txQuery = txQuery.
			WithAfterTxID(afterTxIDParsed).
			WithReferenceFilter(r.URL.Query().Get("reference")).
			WithAccountFilter(r.URL.Query().Get("account")).
			WithSourceFilter(r.URL.Query().Get("source")).
			WithDestinationFilter(r.URL.Query().Get("destination")).
			WithStartTimeFilter(startTimeParsed).
			WithEndTimeFilter(endTimeParsed).
			WithMetadataFilter(sharedapi.GetQueryMap(r.URL.Query(), "metadata")).
			WithPageSize(pageSize)
	}

	cursor, err := l.GetTransactions(r.Context(), *txQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, cursor)
}

type PostTransactionRequest struct {
	Postings  core.Postings `json:"postings"`
	Script    core.Script   `json:"script"`
	Timestamp core.Time     `json:"timestamp"`
	Reference string        `json:"reference"`
	Metadata  core.Metadata `json:"metadata" swaggertype:"object"`
}

func PostTransaction(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	value := r.URL.Query().Get("preview")
	preview := strings.ToUpper(value) == "YES" || strings.ToUpper(value) == "TRUE" || value == "1"

	payload := PostTransactionRequest{}
	if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
		apierrors.ResponseError(w, r,
			ledger.NewValidationError("invalid transaction format"))
		return
	}

	if len(payload.Postings) > 0 && payload.Script.Plain != "" ||
		len(payload.Postings) == 0 && payload.Script.Plain == "" {
		apierrors.ResponseError(w, r, ledger.NewValidationError(
			"invalid payload: should contain either postings or script"))
		return
	} else if len(payload.Postings) > 0 {
		if i, err := payload.Postings.Validate(); err != nil {
			apierrors.ResponseError(w, r, ledger.NewValidationError(errors.Wrap(err,
				fmt.Sprintf("invalid posting %d", i)).Error()))
			return
		}
		txData := core.TransactionData{
			Postings:  payload.Postings,
			Timestamp: payload.Timestamp,
			Reference: payload.Reference,
			Metadata:  payload.Metadata,
		}

		res, err := l.CreateTransaction(r.Context(), preview, core.TxToScriptData(txData))
		if err != nil {
			apierrors.ResponseError(w, r, err)
			return
		}

		sharedapi.Ok(w, res)
		return
	}

	script := core.RunScript{
		Script:    payload.Script,
		Timestamp: payload.Timestamp,
		Reference: payload.Reference,
		Metadata:  payload.Metadata,
	}

	res, err := l.CreateTransaction(r.Context(), preview, script)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, res)
}

func GetTransaction(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	txId, err := strconv.ParseUint(chi.URLParam(r, "txid"), 10, 64)
	if err != nil {
		apierrors.ResponseError(w, r, ledger.NewValidationError("invalid transaction ID"))
		return
	}

	tx, err := l.GetTransaction(r.Context(), txId)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, tx)
}

func RevertTransaction(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	txId, err := strconv.ParseUint(chi.URLParam(r, "txid"), 10, 64)
	if err != nil {
		apierrors.ResponseError(w, r, ledger.NewValidationError("invalid transaction ID"))
		return
	}

	tx, err := l.RevertTransaction(r.Context(), txId)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, tx)
}

func PostTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	var m core.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		apierrors.ResponseError(w, r, ledger.NewValidationError("invalid metadata format"))
		return
	}

	txId, err := strconv.ParseUint(chi.URLParam(r, "txid"), 10, 64)
	if err != nil {
		apierrors.ResponseError(w, r, ledger.NewValidationError("invalid transaction ID"))
		return
	}

	_, err = l.GetTransaction(r.Context(), txId)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	if err := l.SaveMeta(r.Context(), core.MetaTargetTypeTransaction, txId, m); err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.NoContent(w)
}
