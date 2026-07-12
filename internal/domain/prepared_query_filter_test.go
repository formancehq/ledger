package domain_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func mustFilter(t *testing.T, raw string) *commonpb.QueryFilter {
	t.Helper()

	f := &commonpb.QueryFilter{}
	require.NoError(t, json.Unmarshal([]byte(raw), f))

	return f
}

func TestIsPreparedQueryExecutableTarget(t *testing.T) {
	t.Parallel()

	require.True(t, domain.IsPreparedQueryExecutableTarget(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS))
	require.True(t, domain.IsPreparedQueryExecutableTarget(commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS))
	// LOGS is not yet executable as a prepared query (EN-1503); AUDIT never is.
	require.False(t, domain.IsPreparedQueryExecutableTarget(commonpb.QueryTarget_QUERY_TARGET_LOGS))
	require.False(t, domain.IsPreparedQueryExecutableTarget(commonpb.QueryTarget_QUERY_TARGET_AUDIT))
	require.False(t, domain.IsPreparedQueryExecutableTarget(commonpb.QueryTarget(999)))
}

func TestValidateFilterForTarget(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		raw     string
		target  commonpb.QueryTarget
		wantErr string
	}{
		{
			name:   "nil filter is nothing to validate",
			raw:    "", // handled below as a nil filter
			target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		},
		{
			name:   "metadata condition valid on accounts",
			raw:    `{"$exists":{"metadata":"x"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		},
		{
			name:    "transaction-only reference rejected on accounts",
			raw:     `{"$match":{"reference":"r"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "accounts",
		},
		{
			name:   "transaction-only reference valid on transactions",
			raw:    `{"$match":{"reference":"r"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
		},
		{
			name:    "log-only logId rejected on accounts",
			raw:     `{"$gt":{"logId":"5"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "accounts",
		},
		{
			name:    "log-only logId rejected on transactions",
			raw:     `{"$gt":{"logId":"5"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS,
			wantErr: "transactions",
		},
		{
			name:    "invalid condition nested in $and is rejected",
			raw:     `{"$and":[{"$exists":{"metadata":"x"}},{"$gt":{"logId":"5"}}]}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			wantErr: "accounts",
		},
		{
			name:   "combinator with all-valid children passes",
			raw:    `{"$or":[{"$exists":{"metadata":"x"}},{"$match":{"address":"world"}}]}`,
			target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			var f *commonpb.QueryFilter
			if tc.raw != "" {
				f = mustFilter(t, tc.raw)
			}

			err := domain.ValidateFilterForTarget(f, tc.target)
			if tc.wantErr == "" {
				require.Nil(t, err)

				return
			}

			require.NotNil(t, err)
			require.Contains(t, err.Error(), tc.wantErr)
		})
	}
}

// nestFilter wraps a leaf condition in `depth` levels of $not so the resulting
// tree has exactly `depth`+1 nodes on its single path. $not is a structural
// combinator (always valid on ACCOUNTS), so depth — not condition validity — is
// what these cases exercise.
func nestFilter(leaf *commonpb.QueryFilter, depth int) *commonpb.QueryFilter {
	f := leaf
	for range depth {
		f = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: f}},
		}
	}

	return f
}

// TestValidateFilterForTarget_DepthCap pins the write-time recursion cap to the
// same MaxFilterDepth query.Compile enforces at execute time: a tree at the
// limit is accepted, one past it is rejected with ErrFilterTooDeep. Without this
// cap a deeper-but-otherwise-valid filter would be persisted here yet fail every
// execution (and the deep write itself reopens the #341 stack-exhaustion path).
func TestValidateFilterForTarget_DepthCap(t *testing.T) {
	t.Parallel()

	leaf := mustFilter(t, `{"$exists":{"metadata":"x"}}`)

	// The combinator-consumed budget is MaxFilterDepth: a chain of
	// (MaxFilterDepth-1) $not wrappers plus the leaf sits exactly at the limit.
	atLimit := nestFilter(leaf, domain.MaxFilterDepth-1)
	require.Nil(t, domain.ValidateFilterForTarget(atLimit, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS))

	// One level deeper trips the cap before reaching the (valid) leaf.
	overLimit := nestFilter(leaf, domain.MaxFilterDepth)
	err := domain.ValidateFilterForTarget(overLimit, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	require.NotNil(t, err)
	require.ErrorIs(t, err, domain.ErrFilterTooDeep)
}
