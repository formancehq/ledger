package commonpb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// TestTargetConditionValidityIsComplete is the completeness gate that closes the
// bug class this table was introduced to fix (EN-1504): a new condition kind or
// a new query target cannot be added without an explicit per-target validity
// declaration. If someone adds a QueryFilter arm and wires it into
// ConditionKindOf / allConditionKinds without declaring where it is valid, or
// adds a QueryTarget without extending the table, this test fails loudly instead
// of the condition being silently accepted (or rejected) everywhere.
func TestTargetConditionValidityIsComplete(t *testing.T) {
	t.Parallel()

	// Every real kind must have a human-readable name.
	for _, kind := range allConditionKinds {
		name, ok := conditionKindNames[kind]
		require.Truef(t, ok, "ConditionKind %d has no entry in conditionKindNames", int(kind))
		require.NotEmptyf(t, name, "ConditionKind %d has an empty name", int(kind))
	}

	// The table must cover exactly the declared set of targets...
	require.Lenf(t, targetConditionValidity, len(allQueryTargets),
		"targetConditionValidity declares %d targets but allQueryTargets lists %d",
		len(targetConditionValidity), len(allQueryTargets))

	// ...and every (target, kind) pair must be explicitly declared.
	for _, target := range allQueryTargets {
		byKind, ok := targetConditionValidity[target]
		require.Truef(t, ok, "target %s has no validity map", TargetHumanName(target))

		require.Lenf(t, byKind, len(allConditionKinds),
			"target %s declares %d kinds but allConditionKinds lists %d",
			TargetHumanName(target), len(byKind), len(allConditionKinds))

		for _, kind := range allConditionKinds {
			_, present := byKind[kind]
			require.Truef(t, present,
				"target %s is missing an explicit validity entry for condition %q",
				TargetHumanName(target), kind.String())
		}
	}
}

// TestConditionKindOfCoversEveryArm asserts every QueryFilter oneof arm maps to a
// real (non-Unknown) ConditionKind, so a newly added arm that ConditionKindOf
// forgets to handle is caught here rather than silently returning Unknown (and
// being rejected on every target).
func TestConditionKindOfCoversEveryArm(t *testing.T) {
	t.Parallel()

	arms := []struct {
		name   string
		filter *QueryFilter
	}{
		{"field", &QueryFilter{Filter: &QueryFilter_Field{Field: &FieldCondition{}}}},
		{"address", &QueryFilter{Filter: &QueryFilter_Address{Address: &AddressMatch{}}}},
		{"and", &QueryFilter{Filter: &QueryFilter_And{And: &AndFilter{}}}},
		{"or", &QueryFilter{Filter: &QueryFilter_Or{Or: &OrFilter{}}}},
		{"not", &QueryFilter{Filter: &QueryFilter_Not{Not: &NotFilter{}}}},
		{"reference", &QueryFilter{Filter: &QueryFilter_Reference{Reference: &ReferenceCondition{}}}},
		{"reverted", &QueryFilter{Filter: &QueryFilter_Reverted{Reverted: &RevertedCondition{}}}},
		{"accountHasAsset", &QueryFilter{Filter: &QueryFilter_AccountHasAsset{AccountHasAsset: &AccountHasAssetCondition{}}}},
		{"builtinUint", &QueryFilter{Filter: &QueryFilter_BuiltinUint{BuiltinUint: &BuiltinUintCondition{}}}},
		{"logBuiltinUint", &QueryFilter{Filter: &QueryFilter_LogBuiltinUint{LogBuiltinUint: &LogBuiltinUintCondition{}}}},
		{"logId", &QueryFilter{Filter: &QueryFilter_LogId{LogId: &LogIdCondition{}}}},
		{"ledger", &QueryFilter{Filter: &QueryFilter_Ledger{Ledger: &LedgerCondition{}}}},
	}

	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			t.Parallel()

			kind := ConditionKindOf(arm.filter)
			require.NotEqualf(t, ConditionKindUnknown, kind,
				"QueryFilter arm %q maps to ConditionKindUnknown — add it to ConditionKindOf", arm.name)
		})
	}
}

// TestNilFilterIsUnknownAndNeverValid pins that a nil / empty filter maps to
// Unknown and is invalid on every target — a filter the validator does not
// understand is rejected, never silently admitted (invariant #7).
func TestNilFilterIsUnknownAndNeverValid(t *testing.T) {
	t.Parallel()

	require.Equal(t, ConditionKindUnknown, ConditionKindOf(nil))
	require.Equal(t, ConditionKindUnknown, ConditionKindOf(&QueryFilter{}))

	for _, target := range allQueryTargets {
		require.Falsef(t, ConditionValidForTarget(target, ConditionKindUnknown),
			"ConditionKindUnknown must never be valid (target %s)", TargetHumanName(target))
	}
}
