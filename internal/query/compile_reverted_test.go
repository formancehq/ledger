package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// The reverted filter is transaction-only; on any other target it must fail to
// compile rather than silently returning the wrong entities.
func TestCompileRevertedCondition_RejectsNonTransactionTarget(t *testing.T) {
	t.Parallel()

	ctx := &compileCtx{target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS}

	_, err := compile(ctx, &ledgerpb.QueryFilter{
		Filter: &ledgerpb.QueryFilter_Reverted{
			Reverted: &ledgerpb.RevertedCondition{Value: true},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), `condition "reverted" is not valid on target accounts`)
}
