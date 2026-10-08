package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"
)

// Transaction builtins (id/timestamp/insertedAt/revertedAt) are transaction-only;
// on any other target they must fail to compile rather than silently feeding
// transaction-keyed entities into a mismatched result pipeline.
func TestCompileBuiltinUintCondition_RejectsNonTransactionTarget(t *testing.T) {
	t.Parallel()

	for _, target := range []ledgerpb.QueryTarget{
		ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		ledgerpb.QueryTarget_QUERY_TARGET_LOGS,
	} {
		ctx := &compileCtx{target: target}

		_, err := compile(ctx, &ledgerpb.QueryFilter{
			Filter: &ledgerpb.QueryFilter_BuiltinUint{
				BuiltinUint: &ledgerpb.BuiltinUintCondition{
					Field: ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
					Cond:  &ledgerpb.UintCondition{Min: new(uint64(1))},
				},
			},
		})
		require.Error(t, err, "target=%v", target)
		require.Contains(t, err.Error(),
			`condition "builtin field (id/timestamp/insertedAt/revertedAt)" is not valid on target`,
			"target=%v", target)
	}
}
