package v2

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"

	"github.com/formancehq/go-libs/v3/api"

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
	if _, _, err := l.InsertSchema(r.Context(), getCommandParameters(r, ledgercontroller.InsertSchema{
		Data:    data,
		Version: chi.URLParam(r, "version"),
	})); err != nil {
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

	w.WriteHeader(http.StatusNoContent)
}
