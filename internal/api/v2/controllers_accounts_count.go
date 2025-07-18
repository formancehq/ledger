package v2

import (
	"fmt"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
	"net/http"

	"errors"
	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/ledger/internal/api/common"
)

func countAccounts(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	rq, err := getResourceQuery[any](r)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	count, err := l.CountAccounts(r.Context(), *rq)
	if err != nil {
		switch {
		case errors.Is(err, storagecommon.ErrInvalidQuery{}) || errors.Is(err, ledgerstore.ErrMissingFeature{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	w.Header().Set("Count", fmt.Sprint(count))
	api.NoContent(w)
}
