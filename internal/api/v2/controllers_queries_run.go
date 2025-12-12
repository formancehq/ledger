package v2

import (
	"net/http"
	// "strconv"

	"github.com/go-chi/chi/v5"
	_ "github.com/pkg/errors"

	// "github.com/formancehq/go-libs/v3/api"

	// ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
	// ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	// systemcontroller "github.com/formancehq/ledger/internal/controller/system"
)

type RunQueryBody struct {
	Params        map[string]string              `json:"params,omitempty"`
}

func runQuery(w http.ResponseWriter, r *http.Request) {
	common.WithBody(w, r, func(payload RunQueryBody) {
		l := common.LedgerFromContext(r.Context())

		schemaVersion := r.URL.Query().Get("schemaVersion")
		queryId := chi.URLParam(r, "id")

		l.RunQuery(r.Context(), schemaVersion, queryId, payload.Params)

		w.WriteHeader(http.StatusAccepted)
	})
}
