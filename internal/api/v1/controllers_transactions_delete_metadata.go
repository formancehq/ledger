package v1

import (
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v4/api"

	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func deleteTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	transactionID, err := strconv.ParseUint(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, errors.New("invalid transaction ID"))
		return
	}

	metadataKey := chi.URLParam(r, "key")

	_, idempotencyHit, err := l.DeleteTransactionMetadata(r.Context(), getCommandParameters(r, ledgercontroller.DeleteTransactionMetadata{
		TransactionID: transactionID,
		Key:           metadataKey,
	}))
	if err != nil {
		common.HandleCommonWriteErrors(w, r, err)
		return
	}
	if idempotencyHit {
		w.Header().Set("Idempotency-Hit", "true")
	}

	api.NoContent(w)
}
