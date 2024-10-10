package v2

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/formancehq/go-libs/platform/postgres"
	"net/http"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/api"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/api/common"
)

func exportLogs(w http.ResponseWriter, r *http.Request) {
	enc := json.NewEncoder(w)
	w.Header().Set("Content-Type", "application/octet-stream")
	if err := common.LedgerFromContext(r.Context()).Export(r.Context(), ledgercontroller.ExportWriterFn(func(ctx context.Context, log ledger.Log) error {
		return enc.Encode(log)
	})); err != nil {
		switch {
		case errors.Is(err, postgres.ErrTooManyClient{}):
			api.WriteErrorResponse(w, http.StatusServiceUnavailable, api.ErrorInternal, err)
		default:
			api.InternalServerError(w, r, err)
		}
		return
	}
}
