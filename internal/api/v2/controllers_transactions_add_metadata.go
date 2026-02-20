package v2

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v4/api"
	"github.com/formancehq/go-libs/v4/metadata"

	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func addTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	common.WithBody(w, r, func(m metadata.Metadata) {
		txID, err := strconv.ParseUint(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		_, idempotencyHit, err := l.SaveTransactionMetadata(r.Context(), getCommandParameters(r, ledgercontroller.SaveTransactionMetadata{
			TransactionID: txID,
			Metadata:      m,
		}))
		if err != nil {
			switch {
			case errors.Is(err, ledgercontroller.ErrNotFound):
				api.NotFound(w, err)
			default:
				common.HandleCommonWriteErrors(w, r, err)
			}
			return
		}
		if idempotencyHit {
			w.Header().Set("Idempotency-Hit", "true")
		}

		api.NoContent(w)
	})
}
