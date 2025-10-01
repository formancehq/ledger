package v2

import (
	"context"
	"encoding/json"
	"net/http"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/backend"
	api "github.com/formancehq/ledger/internal/api/sharedapi"
	"github.com/formancehq/ledger/internal/engine"
)

func exportLogs(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := backend.LedgerFromContext(r.Context()).Export(r.Context(), engine.ExportWriterFn(func(ctx context.Context, log *ledger.ChainedLog) error {
		return enc.Encode(log)
	})); err != nil {
		api.InternalServerError(w, r, err)
		return
	}
}
