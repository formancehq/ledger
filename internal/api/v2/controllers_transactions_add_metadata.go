package v2

import (
	"net/http"
	"strconv"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"errors"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/go-chi/chi/v5"
)

func addTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	common.WithBody(w, r, func(m metadata.Metadata) {
		txID, err := strconv.ParseUint(chi.URLParam(r, "id"), 10, 64)
		if err != nil {
			api.BadRequest(w, common.ErrValidation, err)
			return
		}

		if _, err := l.SaveTransactionMetadata(r.Context(), getCommandParameters(r, ledgercontroller.SaveTransactionMetadata{
			TransactionID: txID,
			Metadata:      m,
		})); err != nil {
			switch {
			case errors.Is(err, ledgercontroller.ErrNotFound):
				api.NotFound(w, err)
			default:
				common.HandleCommonWriteErrors(w, r, err)
			}
			return
		}

		api.NoContent(w)
	})
}
