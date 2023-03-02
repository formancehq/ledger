package controllers

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/go-chi/chi/v5"
	"github.com/numary/ledger/pkg/api/apierrors"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
)

type LedgerController struct{}

func NewLedgerController() LedgerController {
	return LedgerController{}
}

type Info struct {
	Name    string      `json:"name"`
	Storage storageInfo `json:"storage"`
}

type storageInfo struct {
	Migrations []core.MigrationInfo `json:"migrations"`
}

func (ctl *LedgerController) GetInfo(w http.ResponseWriter, r *http.Request) {
	ledger := LedgerFromContext(r.Context())

	var err error
	res := Info{
		Name:    chi.URLParam(r, "ledger"),
		Storage: storageInfo{},
	}
	res.Storage.Migrations, err = ledger.GetMigrationsInfo(r.Context())
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, res)
}

func (ctl *LedgerController) GetStats(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	stats, err := l.Stats(r.Context())
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, stats)
}

func (ctl *LedgerController) GetLogs(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	logsQuery := ledger.NewLogsQuery()

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		if r.URL.Query().Get("after") != "" ||
			r.URL.Query().Get(QueryKeyStartTime) != "" ||
			r.URL.Query().Get(QueryKeyStartTimeDeprecated) != "" ||
			r.URL.Query().Get(QueryKeyEndTime) != "" ||
			r.URL.Query().Get(QueryKeyEndTimeDeprecated) != "" ||
			r.URL.Query().Get(QueryKeyPageSize) != "" ||
			r.URL.Query().Get(QueryKeyPageSizeDeprecated) != "" {
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

		token := sqlstorage.LogsPaginationToken{}
		if err := json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursor)))
			return
		}

		logsQuery = logsQuery.
			WithAfterID(token.AfterID).
			WithStartTimeFilter(token.StartTime).
			WithEndTimeFilter(token.EndTime).
			WithPageSize(token.PageSize)

	} else if r.URL.Query().Get(QueryKeyCursorDeprecated) != "" {
		if r.URL.Query().Get("after") != "" ||
			r.URL.Query().Get(QueryKeyStartTime) != "" ||
			r.URL.Query().Get(QueryKeyStartTimeDeprecated) != "" ||
			r.URL.Query().Get(QueryKeyEndTime) != "" ||
			r.URL.Query().Get(QueryKeyEndTimeDeprecated) != "" ||
			r.URL.Query().Get(QueryKeyPageSize) != "" ||
			r.URL.Query().Get(QueryKeyPageSizeDeprecated) != "" {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("no other query params can be set with '%s'", QueryKeyCursorDeprecated)))
			return
		}

		res, err := base64.RawURLEncoding.DecodeString(r.URL.Query().Get(QueryKeyCursorDeprecated))
		if err != nil {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursorDeprecated)))
			return
		}

		token := sqlstorage.LogsPaginationToken{}
		if err := json.Unmarshal(res, &token); err != nil {
			apierrors.ResponseError(w, r, ledger.NewValidationError(
				fmt.Sprintf("invalid '%s' query param", QueryKeyCursorDeprecated)))
			return
		}

		logsQuery = logsQuery.
			WithAfterID(token.AfterID).
			WithStartTimeFilter(token.StartTime).
			WithEndTimeFilter(token.EndTime).
			WithPageSize(token.PageSize)

	} else {
		var err error
		var afterIDParsed uint64
		if r.URL.Query().Get("after") != "" {
			afterIDParsed, err = strconv.ParseUint(r.URL.Query().Get("after"), 10, 64)
			if err != nil {
				apierrors.ResponseError(w, r, ledger.NewValidationError(
					"invalid 'after' query param"))
				return
			}
		}

		var startTimeParsed, endTimeParsed time.Time
		if r.URL.Query().Get(QueryKeyStartTime) != "" {
			startTimeParsed, err = time.Parse(time.RFC3339, r.URL.Query().Get(QueryKeyStartTime))
			if err != nil {
				apierrors.ResponseError(w, r, ErrInvalidStartTime)
				return
			}
		}
		if r.URL.Query().Get(QueryKeyStartTimeDeprecated) != "" {
			startTimeParsed, err = time.Parse(time.RFC3339, r.URL.Query().Get(QueryKeyStartTimeDeprecated))
			if err != nil {
				apierrors.ResponseError(w, r, ErrInvalidStartTimeDeprecated)
				return
			}
		}

		if r.URL.Query().Get(QueryKeyEndTime) != "" {
			endTimeParsed, err = time.Parse(time.RFC3339, r.URL.Query().Get(QueryKeyEndTime))
			if err != nil {
				apierrors.ResponseError(w, r, ErrInvalidEndTime)
				return
			}
		}
		if r.URL.Query().Get(QueryKeyEndTimeDeprecated) != "" {
			endTimeParsed, err = time.Parse(time.RFC3339, r.URL.Query().Get(QueryKeyEndTimeDeprecated))
			if err != nil {
				apierrors.ResponseError(w, r, ErrInvalidEndTimeDeprecated)
				return
			}
		}

		pageSize, err := getPageSize(w, r)
		if err != nil {
			apierrors.ResponseError(w, r, err)
			return
		}

		logsQuery = logsQuery.
			WithAfterID(afterIDParsed).
			WithStartTimeFilter(startTimeParsed).
			WithEndTimeFilter(endTimeParsed).
			WithPageSize(pageSize)
	}

	cursor, err := l.GetLogs(r.Context(), logsQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, cursor)
}
