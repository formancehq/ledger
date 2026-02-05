package v1

import (
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"

	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstorage "github.com/formancehq/ledger/internal/storage/ledger"
)

func listAccounts(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	rq, err := getPaginatedQuery(
		r,
		"address",
		bunpaginate.OrderAsc,
		func(resourceQuery *storagecommon.ResourceQuery[any]) error {
			var err error
			resourceQuery.Builder, err = buildAccountsFilterQuery(r)
			return err
		},
	)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	cursor, err := l.ListAccounts(r.Context(), rq)
	if err != nil {
		switch {
		case errors.Is(err, ledgerstorage.ErrMissingFeature{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.RenderCursor(w, *cursor)
}
