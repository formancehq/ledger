package queries

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/pkg/filterexpr"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestUpdateRejectsExplicitlyEmptyFilterBeforeConnecting(t *testing.T) {
	t.Parallel()

	for _, filter := range []string{"", "   \t"} {
		cmd := NewUpdateCommand()
		cmd.SilenceErrors = true
		cmd.SilenceUsage = true
		cmd.SetArgs([]string{"all-accounts", "--ledger", "test", "--filter", filter})

		err := cmd.Execute()
		require.ErrorContains(t, err, "--filter must contain at least one condition")
	}
}

func TestResolveUpdateIDFilter(t *testing.T) {
	t.Parallel()
	for _, raw := range []string{
		`id == 7 or id == 8`,
		`{"$or":[{"$match":{"id":"7"}},{"$match":{"id":"8"}}]}`,
	} {
		provisional, err := filterexpr.DecodeDualFormatStructuralOnly([]byte(raw), commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
		require.NoError(t, err)
		require.True(t, filterexpr.HasTransactionIDCondition(provisional))
		for _, target := range []commonpb.QueryTarget{commonpb.QueryTarget_QUERY_TARGET_LOGS, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS} {
			filter, err := resolveUpdateIDFilter([]byte(raw), []*commonpb.PreparedQuery{{Name: "other", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS}, {Name: "selected", Target: target}}, "selected")
			require.NoError(t, err)
			require.Len(t, filter.GetOr().GetFilters(), 2)
			for _, child := range filter.GetOr().GetFilters() {
				if target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
					require.NotNil(t, child.GetLogId())
				} else {
					require.Equal(t, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID, child.GetBuiltinUint().GetField())
				}
			}
		}
	}
	_, err := resolveUpdateIDFilter([]byte(`id == 7`), nil, "missing")
	require.ErrorContains(t, err, "not found")
}
