package v2

import (
	"net/http"
	"strconv"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/internal/api/common"

	"github.com/formancehq/go-libs/v2/api"
)

func deleteTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	txID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	metadataKey := chi.URLParam(r, "key")

	if _, err := l.DeleteTransactionMetadata(r.Context(), getCommandParameters(r, ledgercontroller.DeleteTransactionMetadata{
		TransactionID: int(txID),
		Key:           metadataKey,
	})); err != nil {
		common.HandleCommonWriteErrors(w, r, err)
		return
	}

	api.NoContent(w)
}
