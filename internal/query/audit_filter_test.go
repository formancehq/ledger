package query

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func uintEq(v uint64) *commonpb.UintCondition {
	return &commonpb.UintCondition{Min: &v, Max: &v}
}

func auditFilter(field commonpb.AuditField, cond any) *commonpb.QueryFilter {
	ac := &commonpb.AuditCondition{Field: field}
	switch c := cond.(type) {
	case *commonpb.UintCondition:
		ac.Condition = &commonpb.AuditCondition_UintCond{UintCond: c}
	case *commonpb.StringCondition:
		ac.Condition = &commonpb.AuditCondition_StringCond{StringCond: c}
	case *commonpb.BoolCondition:
		ac.Condition = &commonpb.AuditCondition_BoolCond{BoolCond: c}
	}
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Audit{Audit: ac}}
}

func strEq(v string) *commonpb.StringCondition {
	return &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: v}}
}

func TestCompileAuditPredicate_HeaderFields(t *testing.T) {
	t.Parallel()

	success := &auditpb.AuditEntry{
		Sequence:   7,
		ProposalId: 42,
		Timestamp:  &commonpb.Timestamp{Data: 1700000000000000000},
		Outcome:    &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{MinLogSequence: 100, MaxLogSequence: 105}},
		Ledgers:    []string{"main", "treasury"},
		CallerSnapshot: &commonpb.CallerSnapshot{
			Identity: &commonpb.CallerIdentity{Subject: "alice"},
			Scopes:   []string{"ledger:read", "ledger:write"},
			God:      true,
		},
	}
	failure := &auditpb.AuditEntry{
		Sequence: 8,
		Outcome:  &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{ErrorType: "insufficient_funds"}},
	}

	tests := []struct {
		name   string
		filter *commonpb.QueryFilter
		entry  *auditpb.AuditEntry
		want   bool
	}{
		{"seq match", auditFilter(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, uintEq(7)), success, true},
		{"seq miss", auditFilter(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, uintEq(9)), success, false},
		{"proposal id", auditFilter(commonpb.AuditField_AUDIT_FIELD_PROPOSAL_ID, uintEq(42)), success, true},
		{"timestamp", auditFilter(commonpb.AuditField_AUDIT_FIELD_TIMESTAMP, uintEq(1700000000000000000)), success, true},
		{"outcome success", auditFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, strEq("success")), success, true},
		{"outcome failure on success", auditFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, strEq("failure")), success, false},
		{"error type", auditFilter(commonpb.AuditField_AUDIT_FIELD_ERROR_TYPE, strEq("insufficient_funds")), failure, true},
		{"error type on success", auditFilter(commonpb.AuditField_AUDIT_FIELD_ERROR_TYPE, strEq("insufficient_funds")), success, false},
		{"caller subject", auditFilter(commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, strEq("alice")), success, true},
		{"caller scope contains", auditFilter(commonpb.AuditField_AUDIT_FIELD_CALLER_SCOPE, strEq("ledger:write")), success, true},
		{"caller scope miss", auditFilter(commonpb.AuditField_AUDIT_FIELD_CALLER_SCOPE, strEq("admin")), success, false},
		{"caller god", auditFilter(commonpb.AuditField_AUDIT_FIELD_CALLER_GOD, &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Hardcoded{Hardcoded: true}}), success, true},
		{"ledger contains", auditFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, strEq("treasury")), success, true},
		{"ledger miss", auditFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, strEq("other")), success, false},
		{"log seq overlap", auditFilter(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, uintEq(103)), success, true},
		{"log seq outside", auditFilter(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, uintEq(200)), success, false},
		{"log seq on failure", auditFilter(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, uintEq(103)), failure, false},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pred, needsItems, err := CompileAuditPredicate(tc.filter)
			require.NoError(t, err)
			require.False(t, needsItems)
			require.Equal(t, tc.want, pred(tc.entry, nil))
		})
	}
}

func TestCompileAuditPredicate_NilMatchesAll(t *testing.T) {
	t.Parallel()
	pred, needsItems, err := CompileAuditPredicate(nil)
	require.NoError(t, err)
	require.False(t, needsItems)
	require.True(t, pred(&auditpb.AuditEntry{}, nil))
}

func TestCompileAuditPredicate_Composition(t *testing.T) {
	t.Parallel()
	entry := &auditpb.AuditEntry{
		Outcome: &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{ErrorType: "x"}},
		Ledgers: []string{"main"},
	}
	and := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{
		auditFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, strEq("failure")),
		auditFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, strEq("main")),
	}}}}
	pred, _, err := CompileAuditPredicate(and)
	require.NoError(t, err)
	require.True(t, pred(entry, nil))

	not := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{
		Filter: auditFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, strEq("success")),
	}}}
	predNot, _, err := CompileAuditPredicate(not)
	require.NoError(t, err)
	require.True(t, predNot(entry, nil))
}

func TestCompileAuditPredicate_Rejections(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name   string
		filter *commonpb.QueryFilter
	}{
		{"non-audit variant", &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Ledger{Ledger: &commonpb.LedgerCondition{Cond: strEq("x")}}}},
		{"field/type mismatch", auditFilter(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, strEq("x"))},
		{"unspecified field", auditFilter(commonpb.AuditField_AUDIT_FIELD_UNSPECIFIED, uintEq(1))},
		{"bad outcome value", auditFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, strEq("maybe"))},
	}
	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := CompileAuditPredicate(tc.filter)
			require.Error(t, err)
		})
	}
}
