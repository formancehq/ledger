package v2

import (
	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
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

		api.RenderCursor(w, *bunpaginate.MapCursor(cursor, func(log ledger.Log) any {
			return renderLog(r, log)
		}))
	}
}
