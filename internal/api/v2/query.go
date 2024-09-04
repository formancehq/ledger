package v2

import (
	"net/http"

	"github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/bun/bunpaginate"
)

const (
	MaxPageSize     = bunpaginate.MaxPageSize
	DefaultPageSize = bunpaginate.QueryDefaultPageSize

	QueryKeyCursor = "cursor"
)

func getCommandParameters(r *http.Request) ledger.Parameters {
	return ledger.Parameters{
		DryRun:         api.QueryParamBool(r, "dryRun"),
		IdempotencyKey: api.IdempotencyKeyFromRequest(r),
	}
}
