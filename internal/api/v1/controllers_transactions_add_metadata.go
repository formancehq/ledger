package v1

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"
	"github.com/formancehq/go-libs/v3/metadata"

	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func addTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		api.BadRequest(w, common.ErrValidation, errors.New("invalid metadata format"))
		return
	}

	txID, err := strconv.ParseUint(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.NotFound(w, errors.New("invalid transaction ID"))
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
}
