package v2

import (
	"net/http"
	"strings"

	"github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/bun/bunpaginate"

	"github.com/formancehq/ledger/v2/internal/engine/command"
	"github.com/pkg/errors"
)

const (
	MaxPageSize     = bunpaginate.MaxPageSize
	DefaultPageSize = bunpaginate.QueryDefaultPageSize

	QueryKeyCursor = "cursor"
)

var (
	ErrInvalidBalanceOperator = errors.New(
		"invalid parameter 'balanceOperator', should be one of 'e, ne, gt, gte, lt, lte'")
	ErrInvalidStartTime = errors.New("invalid 'startTime' query param")
	ErrInvalidEndTime   = errors.New("invalid 'endTime' query param")
)

func getCommandParameters(r *http.Request) command.Parameters {
	dryRunAsString := r.URL.Query().Get("dryRun")
	dryRun := strings.ToUpper(dryRunAsString) == "YES" || strings.ToUpper(dryRunAsString) == "TRUE" || dryRunAsString == "1"

	return command.Parameters{
		DryRun:         dryRun,
		IdempotencyKey: api.IdempotencyKeyFromRequest(r),
	}
}
