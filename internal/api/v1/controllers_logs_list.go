package v1

import (
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/ledger/internal/api/common"
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

	paginatedQuery, err := getColumnPaginatedQuery[any](r, "id", bunpaginate.OrderDesc)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	paginatedQuery.Options.Builder = buildGetLogsQuery(r)

	cursor, err := l.ListLogs(r.Context(), *paginatedQuery)
	if err != nil {
		common.HandleCommonErrors(w, r, err)
		return
	}

	api.RenderCursor(w, *cursor)
}
