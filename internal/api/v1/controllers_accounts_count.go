package v1

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/formancehq/go-libs/v4/api"

	"github.com/formancehq/ledger/internal/api/common"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstorage "github.com/formancehq/ledger/internal/storage/ledger"
)

func countAccounts(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	rq, err := getResourceQuery[any](r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	rq.Builder, err = buildAccountsFilterQuery(r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	count, err := l.CountAccounts(r.Context(), *rq)
	if err != nil {
		switch {
		case errors.Is(err, storagecommon.ErrInvalidQuery{}) || errors.Is(err, ledgerstorage.ErrMissingFeature{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	api.NoContent(w)
}
