package v2

import (
	"encoding/json"
	"errors"
	"net/http"

	"github.com/formancehq/go-libs/v3/api"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/go-chi/chi/v5"
)

func insertSchema(w http.ResponseWriter, r *http.Request) {
	data := ledger.SchemaData{}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		api.BadRequest(w, common.ErrValidation, err)
		return
	}

	l := common.LedgerFromContext(r.Context())
	if _, _, err := l.UpdateSchema(r.Context(), getCommandParameters(r, ledgercontroller.UpdateSchema{
		Data:    data,
		Version: chi.URLParam(r, "version"),
	})); err != nil {
		switch {
		case errors.Is(err, ledgercontroller.ErrSchemaAlreadyExists{}):
			api.BadRequest(w, common.ErrSchemaAlreadyExists, err)
		case errors.Is(err, ledger.ErrInvalidSchema{}):
			api.BadRequest(w, common.ErrValidation, err)
		default:
			common.HandleCommonWriteErrors(w, r, err)
		}
		return
	}

	w.WriteHeader(http.StatusNoContent)
}
