package v2

import (
	"net/http"
	"strconv"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/internal/api/common"
	"github.com/pkg/errors"

	"github.com/formancehq/go-libs/api"
)

func deleteTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	txID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	metadataKey := chi.URLParam(r, "key")

	if err := l.DeleteTransactionMetadata(r.Context(), getCommandParameters(r, ledgercontroller.DeleteTransactionMetadata{
		TransactionID: int(txID),
		Key:           metadataKey,
	})); err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrNotFound):
			api.NotFound(w, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}

	api.NoContent(w)
}
