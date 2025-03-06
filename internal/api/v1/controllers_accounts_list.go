package v1

import (
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func listAccounts(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	rq, err := getOffsetPaginatedQuery[any](r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	rq.Options.Builder, err = buildAccountsFilterQuery(r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	cursor, err := l.ListAccounts(r.Context(), *rq)
	if err != nil {
		switch {
		case errors.Is(err, ledgerstore.ErrMissingFeature{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.RenderCursor(w, *cursor)
}
