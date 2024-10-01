package v2

import (
	"fmt"
	"net/http"

	"errors"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/api/common"
)

func listLogs(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	query := ledgercontroller.GetLogsQuery{}

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		err := bunpaginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &query)
		if err != nil {
			api.BadRequest(w, ErrValidation, fmt.Errorf("invalid '%s' query param", QueryKeyCursor))
			return
		}
	} else {
		var err error

		pageSize, err := bunpaginate.GetPageSize(r)
		if err != nil {
			api.BadRequest(w, ErrValidation, err)
			return
		}

		qb, err := getQueryBuilder(r)
		if err != nil {
			api.BadRequest(w, ErrValidation, err)
			return
		}

		query = ledgercontroller.NewListLogsQuery(ledgercontroller.PaginatedQueryOptions[any]{
			QueryBuilder: qb,
			PageSize:     pageSize,
		})
	}

	cursor, err := l.ListLogs(r.Context(), query)
	if err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrInvalidQuery{}):
			api.BadRequest(w, ErrValidation, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	api.RenderCursor(w, *cursor)
}
