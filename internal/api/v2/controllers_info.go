package v2

import (
	"net/http"

	"github.com/formancehq/ledger/internal/engine/command"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
	"github.com/formancehq/ledger/internal/storage/paginate"
	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/errorsutil"
	"github.com/formancehq/stack/libs/go-libs/migrations"
	"github.com/go-chi/chi/v5"
	"github.com/pkg/errors"
)

type Info struct {
	Name    string      `json:"name"`
	Storage StorageInfo `json:"storage"`
}

type StorageInfo struct {
	Migrations []migrations.Info `json:"migrations"`
}

func getLedgerInfo(w http.ResponseWriter, r *http.Request) {
	ledger := LedgerFromContext(r.Context())

	var err error
	res := Info{
		Name:    chi.URLParam(r, "ledger"),
		Storage: StorageInfo{},
	}
	res.Storage.Migrations, err = ledger.GetMigrationsInfo(r.Context())
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, res)
}

func getStats(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	stats, err := l.Stats(r.Context())
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.Ok(w, stats)
}

func getLogs(w http.ResponseWriter, r *http.Request) {
	l := LedgerFromContext(r.Context())

	query := &ledgerstore.GetLogsQuery{}

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		err := paginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), query)
		if err != nil {
			ResponseError(w, r, errorsutil.NewError(command.ErrValidation,
				errors.Errorf("invalid '%s' query param", QueryKeyCursor)))
			return
		}
	} else {
		var err error

		pageSize, err := getPageSize(r)
		if err != nil {
			ResponseError(w, r, err)
			return
		}

		qb, err := getQueryBuilder(r)
		if err != nil {
			sharedapi.BadRequest(w, ErrValidation, err)
			return
		}

		query = ledgerstore.NewGetLogsQuery(ledgerstore.PaginatedQueryOptions[any]{
			QueryBuilder: qb,
			PageSize:     pageSize,
		})
	}

	cursor, err := l.GetLogs(r.Context(), query)
	if err != nil {
		ResponseError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}
