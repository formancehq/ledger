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
	// LOGS is executable as a prepared query since EN-1503 (query.EnrichLogs).
	require.True(t, domain.IsPreparedQueryExecutableTarget(commonpb.QueryTarget_QUERY_TARGET_LOGS))
	// AUDIT never is (no cursor field, no public target JSON mapping).
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
			name:   "log-only logId valid on logs",
			raw:    `{"$gt":{"logId":"5"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_LOGS,
		},
		{
			name:   "ledger condition valid on logs",
			raw:    `{"$match":{"ledger":"main"}}`,
			target: commonpb.QueryTarget_QUERY_TARGET_LOGS,
		},
		{
			name:    "address rejected on logs (no account→log translation)",
			raw:     `{"$match":{"address":"world"}}`,
			target:  commonpb.QueryTarget_QUERY_TARGET_LOGS,
			wantErr: "logs",
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
