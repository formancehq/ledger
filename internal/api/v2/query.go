package v2

import (
	"net/http"

	"github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/api"
)

func getCommandParameters[INPUT any](r *http.Request, input INPUT) ledger.Parameters[INPUT] {
	return ledger.Parameters[INPUT]{
		DryRun:         api.QueryParamBool(r, "dryRun"),
		IdempotencyKey: api.IdempotencyKeyFromRequest(r),
		Input:          input,
	}
}
