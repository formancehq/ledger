package v2

import (
	"fmt"
	"net/http"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/api/backend"
	"github.com/formancehq/ledger/internal/storage/ledgerstore"
)

func getLogs(w http.ResponseWriter, r *http.Request) {
	l := backend.LedgerFromContext(r.Context())

	query := ledgerstore.GetLogsQuery{}

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		err := bunpaginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &query)
		if err != nil {
			sharedapi.BadRequest(w, ErrValidation, fmt.Errorf("invalid '%s' query param", QueryKeyCursor))
			return
		}
	} else {
		var err error

		pageSize, err := bunpaginate.GetPageSize(r)
		if err != nil {
			sharedapi.BadRequest(w, ErrValidation, err)
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
		sharedapi.InternalServerError(w, r, err)
		return
	}

	sharedapi.RenderCursor(w, *cursor)
}
