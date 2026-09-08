package query

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestCompileReverse_RejectsDeeplyNestedFilter mirrors the forward depth-guard
// test: the descending compiler must bound recursion before reaching a leaf.
func TestCompileReverse_RejectsDeeplyNestedFilter(t *testing.T) {
	t.Parallel()

	var leaf *commonpb.QueryFilter
	filter := leaf

	for range MaxFilterDepth + 5 {
		filter = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Or{
				Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{filter}},
			},
		}
	}

	ctx := &compileCtx{
		target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
	}

	_, err := compileReverse(ctx, filter)
	require.Error(t, err)
	require.True(t, errors.Is(err, ErrFilterTooDeep),
		"deeply-nested QueryFilter must trip the depth guard, got: %v", err)
}

// TestCompileReverse_LedgerConditionOtherLedgerIsEmpty mirrors the forward
// EN-1503 regression for the descending compile path.
func TestCompileReverse_LedgerConditionOtherLedgerIsEmpty(t *testing.T) {
	t.Parallel()

	ctx := &compileCtx{
		target:     commonpb.QueryTarget_QUERY_TARGET_LOGS,
		ledgerName: "ledger-a",
	}

	iter, err := compileReverse(ctx, ledgerFilter("ledger-b"))
	require.NoError(t, err)
	require.NotNil(t, iter)
	defer iter.Close()

	require.False(t, iter.Next(), "LedgerCondition on a different ledger must yield no rows")
	require.NoError(t, iter.Err())
}

// TestCompileReverse_LedgerConditionMissingValue asserts a LedgerCondition
// carrying no value fails loudly on the descending path.
func TestCompileReverse_LedgerConditionMissingValue(t *testing.T) {
	t.Parallel()

	ctx := &compileCtx{
		target:     commonpb.QueryTarget_QUERY_TARGET_LOGS,
		ledgerName: "ledger-a",
	}

	filter := &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Ledger{Ledger: &commonpb.LedgerCondition{}},
	}

	_, err := compileReverse(ctx, filter)
	require.Error(t, err)
}
