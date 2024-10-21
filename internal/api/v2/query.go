package v2

import (
	"net/http"

	"github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/api"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
)

const (
	MaxPageSize     = bunpaginate.MaxPageSize
	DefaultPageSize = bunpaginate.QueryDefaultPageSize

	QueryKeyCursor = "cursor"
)

func getCommandParameters[INPUT any](r *http.Request, input INPUT) ledger.Parameters[INPUT] {
	return ledger.Parameters[INPUT]{
		DryRun:         api.QueryParamBool(r, "dryRun"),
		IdempotencyKey: api.IdempotencyKeyFromRequest(r),
		Input:          input,
	}
}
