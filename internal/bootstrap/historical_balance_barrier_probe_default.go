//go:build !antithesis

package bootstrap

import (
	"context"

	"github.com/formancehq/ledger/v3/internal/infra/node"
)

func antithesisHistoricalBalanceBarrierContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return ctx, func() {}
}

func reachAntithesisHistoricalBalanceBarrierFailure(
	_ context.Context,
	_ *node.Node,
	_ error,
) {
}
