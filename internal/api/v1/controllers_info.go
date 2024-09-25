package v1

import (
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/ledger/v2/internal/api/backend"
	"github.com/formancehq/ledger/v2/internal/engine"
	"github.com/pkg/errors"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/migrations"
	"github.com/formancehq/go-libs/query"
	"github.com/formancehq/ledger/v2/internal/storage/ledgerstore"
)

type Info struct {
	Name    string      `json:"name"`
	Storage StorageInfo `json:"storage"`
}

type StorageInfo struct {
	Migrations []migrations.Info `json:"migrations"`
}

func getLedgerInfo(w http.ResponseWriter, r *http.Request) {
	ledger := backend.LedgerFromContext(r.Context())

	var err error
	res := Info{
		Name:    chi.URLParam(r, "ledger"),
		Storage: StorageInfo{},
	}
	res.Storage.Migrations, err = ledger.GetMigrationsInfo(r.Context())
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.Ok(w, res)
}

func getStats(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	stats, err := l.Stats(r.Context())
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.Ok(w, stats)
}

func buildGetLogsQuery(r *http.Request) (query.Builder, error) {
	clauses := make([]query.Builder, 0)
	if after := r.URL.Query().Get("after"); after != "" {
		clauses = append(clauses, query.Lt("id", after))
	}

	if startTime := r.URL.Query().Get("start_time"); startTime != "" {
		clauses = append(clauses, query.Gte("date", startTime))
	}
	if endTime := r.URL.Query().Get("end_time"); endTime != "" {
		clauses = append(clauses, query.Lt("date", endTime))
	}

	if len(clauses) == 0 {
		return nil, nil
	}
	if len(clauses) == 1 {
		return clauses[0], nil
	}

	return query.And(clauses...), nil
}

func getLogs(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	query := ledgerstore.GetLogsQuery{}

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		err := bunpaginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &query)
		if err != nil {
			sharedapi.BadRequest(w, ErrValidation, errors.Errorf("invalid '%s' query param", QueryKeyCursor))
			return
		}
	} else {
		var err error

		pageSize, err := bunpaginate.GetPageSize(r,
			bunpaginate.WithDefaultPageSize(DefaultPageSize),
			bunpaginate.WithMaxPageSize(MaxPageSize))
		if err != nil {
			switch {
			case engine.IsStorageError(err):
				sharedapi.BadRequest(w, ErrValidation, err)
			default:
				sharedapi.InternalServerError(w, r, err)
			}
			return
		}

		qb, err := buildGetLogsQuery(r)
		if err != nil {
			sharedapi.BadRequest(w, ErrValidation, err)
			return
		}

		query = ledgerstore.NewGetLogsQuery(ledgerstore.PaginatedQueryOptions[any]{
			QueryBuilder: qb,
			PageSize:     uint64(pageSize),
		})
	}

	cursor, err := l.GetLogs(r.Context(), query)
	if err != nil {
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}
