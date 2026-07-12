package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// The reverted filter is transaction-only; on any other target it must fail to
// compile rather than silently returning the wrong entities.
func TestCompileRevertedCondition_RejectsNonTransactionTarget(t *testing.T) {
	t.Parallel()

	ctx := &compileCtx{target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS}

	_, err := compile(ctx, &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Reverted{
			Reverted: &commonpb.RevertedCondition{Value: true},
		},
	})
	require.Error(t, err)
	require.Contains(t, err.Error(), `condition "reverted" is not valid on target accounts`)
}
