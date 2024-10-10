package v1

import (
	"net/http"
	"strconv"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"errors"
	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/go-chi/chi/v5"
)

func deleteTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	transactionID, err := strconv.ParseUint(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.BadRequest(w, ErrValidation, errors.New("invalid transaction ID"))
		return
	}

	metadataKey := chi.URLParam(r, "key")

	if err := l.DeleteTransactionMetadata(r.Context(), getCommandParameters(r, ledgercontroller.DeleteTransactionMetadata{
		TransactionID: int(transactionID),
		Key:           metadataKey,
	})); err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrNotFound):
			api.NotFound(w, err)
		default:
			common.HandleCommonErrors(w, r, err)
		}
		return
	}

	api.NoContent(w)
}
