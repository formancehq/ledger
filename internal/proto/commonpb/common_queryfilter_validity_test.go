package commonpb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// queryFilterOneofArms returns the descriptors of every arm of the
// QueryFilter.filter oneof, read from the compiled proto descriptor. Deriving
// the expected set from the descriptor (rather than a hand-maintained list) is
// what makes the completeness gate real: a new arm added to the .proto shows up
// here automatically and must be handled by the generated table.
func queryFilterOneofArms(t *testing.T) []protoreflect.FieldDescriptor {
	t.Helper()

	md := (&QueryFilter{}).ProtoReflect().Descriptor()

	oneof := md.Oneofs().ByName("filter")
	require.NotNil(t, oneof, "QueryFilter.filter oneof not found in descriptor")

	fields := oneof.Fields()
	arms := make([]protoreflect.FieldDescriptor, 0, fields.Len())
	for i := range fields.Len() {
		arms = append(arms, fields.Get(i))
	}

	return arms
}

// queryTargetValues returns every QueryTarget enum value from the descriptor.
func queryTargetValues(t *testing.T) []QueryTarget {
	t.Helper()

	ed := QueryTarget(0).Descriptor().Values()
	targets := make([]QueryTarget, 0, ed.Len())
	for i := range ed.Len() {
		targets = append(targets, QueryTarget(ed.Get(i).Number()))
	}

	return targets
}

// TestConditionKindsCoverEveryOneofArm asserts that every arm of the
// QueryFilter.filter oneof — enumerated from the proto descriptor — maps to a
// distinct, real (non-Unknown) ConditionKind via ConditionKindOf, and that the
// generated allConditionKinds has exactly one entry per arm. So a new arm added
// to the .proto that the generator does not handle (or handles ambiguously)
// fails here, closing the bug class rather than the instance.
func TestConditionKindsCoverEveryOneofArm(t *testing.T) {
	t.Parallel()

	arms := queryFilterOneofArms(t)

	require.Len(t, allConditionKinds, len(arms),
		"allConditionKinds must have exactly one entry per QueryFilter oneof arm")

	seen := make(map[ConditionKind]bool, len(arms))
	for _, arm := range arms {
		msg := (&QueryFilter{}).ProtoReflect()
		// Build a QueryFilter whose oneof is set to this arm, so ConditionKindOf
		// exercises the real type switch on the concrete wrapper type.
		msg.Set(arm, defaultValueForArm(t, msg, arm))

		kind := ConditionKindOf(msg.Interface().(*QueryFilter))
		require.NotEqualf(t, ConditionKindUnknown, kind,
			"oneof arm %q maps to ConditionKindUnknown — the generator must handle it", arm.Name())
		require.Falsef(t, seen[kind], "two oneof arms map to the same ConditionKind %s", kind)
		seen[kind] = true
	}
}

// defaultValueForArm builds a minimal valid value for a oneof arm so it can be
// set on the message (message arms need a non-nil sub-message).
func defaultValueForArm(t *testing.T, msg protoreflect.Message, arm protoreflect.FieldDescriptor) protoreflect.Value {
	t.Helper()

	require.Equal(t, protoreflect.MessageKind, arm.Kind(),
		"every QueryFilter oneof arm is expected to be a message")

	return protoreflect.ValueOfMessage(msg.NewField(arm).Message())
}

// TestTargetConditionValidityIsComplete asserts the generated validity table
// covers the full cross-product of QueryTargets (from the enum descriptor) and
// ConditionKinds (from allConditionKinds), each with an explicit verdict, and
// that every kind has a name. A new target or kind that the generator forgets to
// place in the table fails here.
func TestTargetConditionValidityIsComplete(t *testing.T) {
	t.Parallel()

	targets := queryTargetValues(t)

	// The generated allQueryTargets must match the enum descriptor exactly.
	require.ElementsMatch(t, targets, allQueryTargets,
		"allQueryTargets must list exactly the QueryTarget enum values")

	require.Len(t, targetConditionValidity, len(targets),
		"targetConditionValidity must have one row per QueryTarget")

	for _, kind := range allConditionKinds {
		name, ok := conditionKindNames[kind]
		require.Truef(t, ok, "ConditionKind %d has no name", int(kind))
		require.NotEmptyf(t, name, "ConditionKind %d has an empty name", int(kind))
	}

	for _, target := range targets {
		byKind, ok := targetConditionValidity[target]
		require.Truef(t, ok, "target %s has no validity row", TargetHumanName(target))
		require.Lenf(t, byKind, len(allConditionKinds),
			"target %s must decide every ConditionKind", TargetHumanName(target))

		for _, kind := range allConditionKinds {
			_, present := byKind[kind]
			require.Truef(t, present,
				"target %s is missing an explicit validity entry for %q",
				TargetHumanName(target), kind.String())
		}
	}
}

// TestConditionValidityMatchesExpectedMatrix pins the exact per-target verdicts
// (EN-1504 behavior preservation + the address/reference-on-LOGS correctness
// fix). This is the human-readable contract the proto annotations must satisfy;
// if an annotation changes, this test must be updated deliberately.
func TestConditionValidityMatchesExpectedMatrix(t *testing.T) {
	t.Parallel()

	type row struct {
		kind     ConditionKind
		accounts bool
		txs      bool
		logs     bool
		audit    bool
	}

	// accounts / transactions / logs / audit
	want := []row{
		{ConditionKindField, true, true, true, false},             // metadata: accounts/tx/logs (per-target index/schema checked separately)
		{ConditionKindAddress, true, true, false, false},          // no account→log translation → not on LOGS
		{ConditionKindAnd, true, true, true, true},                // and: every compiler, incl. audit
		{ConditionKindOr, true, true, true, true},                 // or:  every compiler, incl. audit
		{ConditionKindNot, true, true, true, false},               // not: query.Compile yes; CompileAuditFilter rejects it
		{ConditionKindReference, false, true, false, false},       // tx reference index → TX only
		{ConditionKindBuiltinUint, false, true, false, false},     // id/timestamp/insertedAt/revertedAt → TX only
		{ConditionKindLedger, false, false, true, false},          // log-only
		{ConditionKindLogId, false, false, true, false},           // log-only
		{ConditionKindLogBuiltinUint, false, false, true, false},  // date → log-only
		{ConditionKindAccountHasAsset, true, false, false, false}, // accounts only
		{ConditionKindReverted, false, true, false, false},        // reversion bitset → TX only
		{ConditionKindAudit, false, false, false, true},           // audit-only (compiled by CompileAuditFilter)
	}

	for _, r := range want {
		require.Equalf(t, r.accounts, ConditionValidForTarget(QueryTarget_QUERY_TARGET_ACCOUNTS, r.kind),
			"%s on accounts", r.kind)
		require.Equalf(t, r.txs, ConditionValidForTarget(QueryTarget_QUERY_TARGET_TRANSACTIONS, r.kind),
			"%s on transactions", r.kind)
		require.Equalf(t, r.logs, ConditionValidForTarget(QueryTarget_QUERY_TARGET_LOGS, r.kind),
			"%s on logs", r.kind)
		require.Equalf(t, r.audit, ConditionValidForTarget(QueryTarget_QUERY_TARGET_AUDIT, r.kind),
			"%s on audit", r.kind)
	}
}

// TestNilFilterIsUnknownAndNeverValid pins that a nil / empty filter maps to
// Unknown and is invalid on every target — a filter the validator does not
// understand is rejected, never silently admitted (invariant #7).
func TestNilFilterIsUnknownAndNeverValid(t *testing.T) {
	t.Parallel()

	require.Equal(t, ConditionKindUnknown, ConditionKindOf(nil))
	require.Equal(t, ConditionKindUnknown, ConditionKindOf(&QueryFilter{}))

	for _, target := range queryTargetValues(t) {
		require.Falsef(t, ConditionValidForTarget(target, ConditionKindUnknown),
			"ConditionKindUnknown must never be valid (target %s)", TargetHumanName(target))
	}
}
