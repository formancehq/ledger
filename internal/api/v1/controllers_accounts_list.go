package v1

import (
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func listAccounts(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	rqBuilder, err := buildAccountsFilterQuery(r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	rq, err := getPaginatedQuery(r, "address", bunpaginate.OrderAsc, func(resourceQuery *storagecommon.ResourceQuery[any]) {
		resourceQuery.Builder = rqBuilder
	})
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	cursor, err := l.ListAccounts(r.Context(), rq)
	if err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrMissingFeature{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.RenderCursor(w, *cursor)
}
