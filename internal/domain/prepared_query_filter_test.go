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

// TestValidateFilterForTarget_RejectsDeeplyNestedFilter is the regression for
// the EN-1503 review P1: ValidateFilterForTarget runs at write time (admission
// + FSM) on an untrusted gRPC filter, before query.Compile's depth guard can
// fire. Without its own bound, a hostile deeply-nested $and/$or/$not tree would
// blow the Go stack here — an unrecoverable, fatal DoS. Build a chain deeper
// than domain.MaxFilterDepth and assert it is rejected loudly rather than
// recursing. Both CreatePreparedQuery and UpdatePreparedQuery route their
// untrusted filter through this same function, so one guard covers both paths.
func TestValidateFilterForTarget_RejectsDeeplyNestedFilter(t *testing.T) {
	t.Parallel()

	build := func(wrap func(child *commonpb.QueryFilter) *commonpb.QueryFilter) *commonpb.QueryFilter {
		// Innermost leaf is a valid LOGS condition so, absent the depth guard,
		// the walk would traverse the whole chain and succeed.
		var f *commonpb.QueryFilter = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_LogId{
				LogId: &commonpb.LogIdCondition{Cond: &commonpb.UintCondition{}},
			},
		}

		for range domain.MaxFilterDepth + 5 {
			f = wrap(f)
		}

		return f
	}

	andWrap := func(child *commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{
			And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{child}},
		}}
	}
	orWrap := func(child *commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{
			Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{child}},
		}}
	}
	notWrap := func(child *commonpb.QueryFilter) *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{
			Not: &commonpb.NotFilter{Filter: child},
		}}
	}

	for name, wrap := range map[string]func(*commonpb.QueryFilter) *commonpb.QueryFilter{
		"and": andWrap,
		"or":  orWrap,
		"not": notWrap,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			err := domain.ValidateFilterForTarget(build(wrap), commonpb.QueryTarget_QUERY_TARGET_LOGS)
			require.NotNil(t, err, "deeply-nested %s filter must trip the depth guard", name)
			require.Contains(t, err.Error(), "nesting depth")
		})
	}

	// A tree exactly at the limit is accepted (guard is off-by-one safe).
	atLimit := func() *commonpb.QueryFilter {
		var f *commonpb.QueryFilter = &commonpb.QueryFilter{
			Filter: &commonpb.QueryFilter_LogId{
				LogId: &commonpb.LogIdCondition{Cond: &commonpb.UintCondition{}},
			},
		}
		// MaxFilterDepth combinator levels wrapping a leaf: the deepest
		// recursion enters the guard at depth == MaxFilterDepth-1, still under
		// the cap.
		for range domain.MaxFilterDepth {
			f = andWrap(f)
		}

		return f
	}()
	require.Nil(t, domain.ValidateFilterForTarget(atLimit, commonpb.QueryTarget_QUERY_TARGET_LOGS))
}
