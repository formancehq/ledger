package v2

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v4/api"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

func insertSchema(w http.ResponseWriter, r *http.Request) {
	data := ledger.SchemaData{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	l := common.LedgerFromContext(r.Context())
	_, _, idempotencyHit, err := l.InsertSchema(r.Context(), getCommandParameters(r, ledgercontroller.InsertSchema{
		Data:    data,
		Version: chi.URLParam(r, "version"),
	}))
	if err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrSchemaAlreadyExists{}):
			api.WriteErrorResponse(w, http.StatusConflict, common.ErrSchemaAlreadyExists, err)
		case errors.Is(err, ledger.ErrInvalidSchema{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonWriteErrors(w, r, err)
		}
		return
	}
	if idempotencyHit {
		w.Header().Set("Idempotency-Hit", "true")
	}

	w.WriteHeader(http.StatusNoContent)
}
