package v2

import (
	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"net/http"
)

func listLogs(paginationConfig common.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())

		rq, err := getColumnPaginatedQuery[any](r, paginationConfig, "id", bunpaginate.OrderDesc)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		cursor, err := l.ListLogs(r.Context(), *rq)
		if err != nil {
			switch {
			case errors.Is(err, storagecommon.ErrInvalidQuery{}):
				api.BadRequest(w, common.ErrValidation, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}

		api.RenderCursor(w, *cursor)
	}
}
