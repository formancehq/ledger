package v1

import (
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func buildGetLogsQuery(r *http.Request) query.Builder {
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
		return nil
	}
	if len(clauses) == 1 {
		return clauses[0]
	}

	return query.And(clauses...)
}

func getLogs(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	query := ledgercontroller.GetLogsQuery{}

	if r.URL.Query().Get(QueryKeyCursor) != "" {
		err := bunpaginate.UnmarshalCursor(r.URL.Query().Get(QueryKeyCursor), &query)
		if err != nil {
			api.BadRequest(w, ErrValidation, fmt.Errorf("invalid '%s' query param: %w", QueryKeyCursor, err))
			return
		}
	} else {
		var err error

		pageSize, err := bunpaginate.GetPageSize(r,
			bunpaginate.WithDefaultPageSize(DefaultPageSize),
			bunpaginate.WithMaxPageSize(MaxPageSize))
		if err != nil {
			common.HandleCommonErrors(w, r, err)
			return
		}

		query = ledgercontroller.NewListLogsQuery(ledgercontroller.PaginatedQueryOptions[any]{
			QueryBuilder: buildGetLogsQuery(r),
			PageSize:     pageSize,
		})
	}

	cursor, err := l.ListLogs(r.Context(), query)
	if err != nil {
		common.HandleCommonErrors(w, r, err)
		return
	}

	api.RenderCursor(w, *cursor)
}
