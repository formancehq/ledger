package v2

import (
	"encoding/json"
	"net/http"
	"strconv"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"errors"
	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/metadata"
	"github.com/formancehq/ledger/internal/api/common"
	"github.com/go-chi/chi/v5"
)

func addTransactionMetadata(w http.ResponseWriter, r *http.Request) {
	l := common.LedgerFromContext(r.Context())

	var m metadata.Metadata
	if err := json.NewDecoder(r.Body).Decode(&m); err != nil {
		api.BadRequest(w, ErrValidation, errors.New("invalid metadata format"))
		return
	}

	txID, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		api.BadRequest(w, ErrValidation, err)
		return
	}

	if err := l.SaveTransactionMetadata(r.Context(), getCommandParameters(r, ledgercontroller.SaveTransactionMetadata{
		TransactionID: int(txID),
		Metadata:      m,
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
