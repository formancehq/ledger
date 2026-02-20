package v2

import (
	"net/http"

	"github.com/formancehq/go-libs/v4/api"

	"github.com/formancehq/ledger/internal/controller/ledger"
)

func getCommandParameters[INPUT any](r *http.Request, input INPUT) ledger.Parameters[INPUT] {
	return ledger.Parameters[INPUT]{
		DryRun:         api.QueryParamBool(r, "dryRun"),
		SchemaVersion:  r.URL.Query().Get("schemaVersion"),
		IdempotencyKey: api.IdempotencyKeyFromRequest(r),
		Input:          input,
	}
}
