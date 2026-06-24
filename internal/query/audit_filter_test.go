package query

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
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

func strParam(p string) *commonpb.StringCondition {
	return &commonpb.StringCondition{Value: &commonpb.StringCondition_Param{Param: p}}
}

func boolParam(p string) *commonpb.BoolCondition {
	return &commonpb.BoolCondition{Value: &commonpb.BoolCondition_Param{Param: p}}
}

// mustMatch runs pred and fails the test on a predicate error, returning the
// match result. Predicates only error on per-entry data faults (e.g. a corrupt
// audit order), which the dedicated tests assert explicitly.
func mustMatch(t *testing.T, pred AuditPredicate, entry *auditpb.AuditEntry, items []*auditpb.AuditItem) bool {
	t.Helper()
	ok, err := pred(entry, items)
	require.NoError(t, err)

	return ok
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
		Outcome:  &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{Reason: commonpb.ErrorReason_ERROR_REASON_INSUFFICIENT_FUNDS}},
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
		{"error type", auditFilter(commonpb.AuditField_AUDIT_FIELD_ERROR_TYPE, strEq("INSUFFICIENT_FUNDS")), failure, true},
		{"error type on success", auditFilter(commonpb.AuditField_AUDIT_FIELD_ERROR_TYPE, strEq("INSUFFICIENT_FUNDS")), success, false},
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
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			pred, needsItems, err := CompileAuditPredicate(tc.filter)
			require.NoError(t, err)
			require.False(t, needsItems)
			require.Equal(t, tc.want, mustMatch(t, pred, tc.entry, nil))
		})
	}
}

func TestCompileAuditPredicate_NilMatchesAll(t *testing.T) {
	t.Parallel()
	pred, needsItems, err := CompileAuditPredicate(nil)
	require.NoError(t, err)
	require.False(t, needsItems)
	require.True(t, mustMatch(t, pred, &auditpb.AuditEntry{}, nil))
}

func TestCompileAuditPredicate_Composition(t *testing.T) {
	t.Parallel()
	entry := &auditpb.AuditEntry{
		Outcome: &auditpb.AuditEntry_Failure{Failure: &auditpb.AuditFailure{Reason: commonpb.ErrorReason_ERROR_REASON_VALIDATION}},
		Ledgers: []string{"main"},
	}
	and := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: []*commonpb.QueryFilter{
		auditFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, strEq("failure")),
		auditFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, strEq("main")),
	}}}}
	pred, _, err := CompileAuditPredicate(and)
	require.NoError(t, err)
	require.True(t, mustMatch(t, pred, entry, nil))

	not := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{
		Filter: auditFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, strEq("success")),
	}}}
	predNot, _, err := CompileAuditPredicate(not)
	require.NoError(t, err)
	require.True(t, mustMatch(t, predNot, entry, nil))

	// Or is the lowered form of `audit[ledger] in (other, main)`: matches when
	// any branch matches.
	or := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{
		auditFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, strEq("other")),
		auditFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, strEq("main")),
	}}}}
	predOr, _, err := CompileAuditPredicate(or)
	require.NoError(t, err)
	require.True(t, mustMatch(t, predOr, entry, nil))

	// Or with no matching branch is false.
	orMiss := &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: []*commonpb.QueryFilter{
		auditFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, strEq("other")),
		auditFilter(commonpb.AuditField_AUDIT_FIELD_LEDGER, strEq("nope")),
	}}}}
	predOrMiss, _, err := CompileAuditPredicate(orMiss)
	require.NoError(t, err)
	require.False(t, mustMatch(t, predOrMiss, entry, nil))
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
		{"parameterized string field", auditFilter(commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, strParam("user"))},
		{"parameterized outcome", auditFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, strParam("o"))},
		{"parameterized order_type", auditFilter(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, strParam("t"))},
		{"parameterized caller.god", auditFilter(commonpb.AuditField_AUDIT_FIELD_CALLER_GOD, boolParam("g"))},
		{"unset bool oneof caller.god", auditFilter(commonpb.AuditField_AUDIT_FIELD_CALLER_GOD, &commonpb.BoolCondition{})},
		{"unset string oneof caller.subject", auditFilter(commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, &commonpb.StringCondition{})},
		{"unset string oneof outcome", auditFilter(commonpb.AuditField_AUDIT_FIELD_OUTCOME, &commonpb.StringCondition{})},
		{"unset string oneof order_type", auditFilter(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, &commonpb.StringCondition{})},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, _, err := CompileAuditPredicate(tc.filter)
			require.Error(t, err)
		})
	}
}

func itemWithOrder(t *testing.T, order *raftcmdpb.Order) *auditpb.AuditItem {
	t.Helper()
	b, err := proto.Marshal(order)
	require.NoError(t, err)

	return &auditpb.AuditItem{SerializedOrder: b}
}

func TestCompileAuditPredicate_OrderType(t *testing.T) {
	t.Parallel()

	applyOrder := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger:  "main",
			Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{}},
		},
	}}
	numscriptOrder := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
		LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger:  "main",
			Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{SaveNumscript: &raftcmdpb.SaveNumscriptOrder{}},
		},
	}}

	entry := &auditpb.AuditEntry{Sequence: 1}
	items := []*auditpb.AuditItem{itemWithOrder(t, applyOrder), itemWithOrder(t, numscriptOrder)}

	pred, needsItems, err := CompileAuditPredicate(
		auditFilter(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, strEq("apply")))
	require.NoError(t, err)
	require.True(t, needsItems)
	require.True(t, mustMatch(t, pred, entry, items))      // match-any: apply present
	require.False(t, mustMatch(t, pred, entry, items[1:])) // only numscript -> no match

	predMiss, _, err := CompileAuditPredicate(
		auditFilter(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, strEq("create_ledger")))
	require.NoError(t, err)
	require.False(t, mustMatch(t, predMiss, entry, items))
}

// A corrupt SerializedOrder in the audit zone (the cryptographic source of
// truth) must surface as an error from the predicate, not be silently dropped.
func TestCompileAuditPredicate_OrderTypeCorruptBytesSurfacesError(t *testing.T) {
	t.Parallel()

	pred, needsItems, err := CompileAuditPredicate(
		auditFilter(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, strEq("apply")))
	require.NoError(t, err)
	require.True(t, needsItems)

	// Tag 0 (field number 0) is illegal in protobuf, so UnmarshalVT fails.
	corrupt := []*auditpb.AuditItem{{SerializedOrder: []byte{0x00}}}
	_, perr := pred(&auditpb.AuditEntry{Sequence: 1}, corrupt)
	require.Error(t, perr)
}
