package v2

import (
	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"net/http"
)

func listAccounts(paginationConfig common.PaginationConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		l := common.LedgerFromContext(r.Context())

		query, err := getOffsetPaginatedQuery[any](r, paginationConfig)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		cursor, err := l.ListAccounts(r.Context(), *query)
		if err != nil {
			switch {
			case errors.Is(err, storagecommon.ErrInvalidQuery{}) || errors.Is(err, ledgercontroller.ErrMissingFeature{}):
				api.BadRequest(w, common.ErrValidation, err)
			default:
				common.HandleCommonErrors(w, r, err)
			}
			return
		}

		api.RenderCursor(w, *cursor)
	}
}
