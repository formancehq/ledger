package controllers

import (
	"net/http"

	"github.com/formancehq/ledger/pkg/api/apierrors"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

type Info struct {
	Name    string      `json:"name"`
	Storage StorageInfo `json:"storage"`
}

type StorageInfo struct {
	Migrations []core.MigrationInfo `json:"migrations"`
}

func GetLedgerInfo(w http.ResponseWriter, r *http.Request) {
	ledger := LedgerFromContext(r.Context())

	var err error
	res := Info{
		Name:    chi.URLParam(r, "ledger"),
		Storage: StorageInfo{},
	}
	res.Storage.Migrations, err = ledger.GetMigrationsInfo(r.Context())
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, res)
}

func GetStats(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	stats, err := l.Stats(r.Context())
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, stats)
}

func GetLogs(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	logsQuery := storage.NewLogsQuery()

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		if r.URL.Query().Get(QueryKeyStartTime) != "" ||
			r.URL.Query().Get(QueryKeyEndTime) != "" ||
			r.URL.Query().Get(QueryKeyPageSize) != "" {
			apierrors.ResponseError(w, r, errorsutil.NewError(ledger.ErrValidation,
				errors.Errorf("no other query params can be set with '%s'", QueryKeyCursor)))
			return
		}

		err := storage.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &logsQuery)
		if err != nil {
			apierrors.ResponseError(w, r, errorsutil.NewError(ledger.ErrValidation,
				errors.Errorf("invalid '%s' query param", QueryKeyCursor)))
			return
		}
	} else {
		var err error

		var startTimeParsed, endTimeParsed core.Time
		if r.URL.Query().Get(QueryKeyStartTime) != "" {
			startTimeParsed, err = core.ParseTime(r.URL.Query().Get(QueryKeyStartTime))
			if err != nil {
				apierrors.ResponseError(w, r, errorsutil.NewError(ledger.ErrValidation, ErrInvalidStartTime))
				return
			}
		}

		if r.URL.Query().Get(QueryKeyEndTime) != "" {
			endTimeParsed, err = core.ParseTime(r.URL.Query().Get(QueryKeyEndTime))
			if err != nil {
				apierrors.ResponseError(w, r, errorsutil.NewError(ledger.ErrValidation, ErrInvalidEndTime))
				return
			}
		}

		pageSize, err := getPageSize(r)
		if err != nil {
			apierrors.ResponseError(w, r, err)
			return
		}

		logsQuery = logsQuery.
			WithStartTimeFilter(startTimeParsed).
			WithEndTimeFilter(endTimeParsed).
			WithPageSize(pageSize)
	}

	cursor, err := l.GetLogs(r.Context(), logsQuery)
	if err != nil {
		apierrors.ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}
