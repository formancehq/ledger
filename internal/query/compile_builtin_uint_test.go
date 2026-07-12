package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Transaction builtins (id/timestamp/insertedAt/revertedAt) are transaction-only;
// on any other target they must fail to compile rather than silently feeding
// transaction-keyed entities into a mismatched result pipeline.
func TestCompileBuiltinUintCondition_RejectsNonTransactionTarget(t *testing.T) {
	t.Parallel()

	for _, target := range []commonpb.QueryTarget{
		commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		commonpb.QueryTarget_QUERY_TARGET_LOGS,
	} {
		ctx := &compileCtx{target: target}

		_, err := compile(ctx, &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_BuiltinUint{
				BuiltinUint: &commonpb.BuiltinUintCondition{
					Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
					Cond:  &commonpb.UintCondition{Min: new(uint64(1))},
				},
			},
		})
		require.Error(t, err, "target=%v", target)
		require.Contains(t, err.Error(), "only valid on transactions", "target=%v", target)
	}
}
