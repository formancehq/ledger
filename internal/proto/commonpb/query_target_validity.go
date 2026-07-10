package commonpb

import "fmt"

// ConditionKind enumerates the top-level arms of a QueryFilter oneof, at the
// granularity that per-target validity is decided. It is the single vocabulary
// shared by the compile layer (internal/query) and the REST decode layer
// (internal/adapter/http) so both agree by construction about which condition is
// valid on which QueryTarget.
//
// Combinators (And/Or/Not) are structural — they carry no target semantics of
// their own and simply recurse into their children — so they are always valid
// and are represented by ConditionKindCombinator.
type ConditionKind int

const (
	// ConditionKindUnknown is the zero value; it never corresponds to a real
	// QueryFilter arm and always fails validation. It exists so a nil / empty
	// filter, or a newly added arm that ConditionKindOf does not yet map, is
	// caught loudly rather than treated as valid.
	ConditionKindUnknown ConditionKind = iota
	// ConditionKindCombinator covers And/Or/Not — structural nodes valid on
	// every target.
	ConditionKindCombinator
	// ConditionKindField is a metadata field condition ($match/$gt/$exists on
	// metadata[<key>]). Per-target index/schema availability is still enforced
	// by the compiler; this table only decides target eligibility.
	ConditionKindField
	// ConditionKindAddress is an address match ($match on address/source/
	// destination).
	ConditionKindAddress
	// ConditionKindReference is a transaction reference match.
	ConditionKindReference
	// ConditionKindReverted is the transaction revert-status filter.
	ConditionKindReverted
	// ConditionKindAccountHasAsset is the account has-asset filter.
	ConditionKindAccountHasAsset
	// ConditionKindBuiltinUint covers the transaction builtin uint fields
	// (id/timestamp/insertedAt/revertedAt).
	ConditionKindBuiltinUint
	// ConditionKindLogBuiltinUint covers the log builtin uint fields (date).
	ConditionKindLogBuiltinUint
	// ConditionKindLogID is the log-id filter.
	ConditionKindLogID
	// ConditionKindLedger is the ledger filter (log-only, no-op at compile).
	ConditionKindLedger
)

// conditionKindNames gives each kind a human-readable label used in validation
// error messages. Kept in sync with allConditionKinds by the completeness test.
var conditionKindNames = map[ConditionKind]string{
	ConditionKindUnknown:         "unknown",
	ConditionKindCombinator:      "combinator",
	ConditionKindField:           "metadata",
	ConditionKindAddress:         "address",
	ConditionKindReference:       "reference",
	ConditionKindReverted:        "reverted",
	ConditionKindAccountHasAsset: "accountHasAsset",
	ConditionKindBuiltinUint:     "builtin field (id/timestamp/insertedAt/revertedAt)",
	ConditionKindLogBuiltinUint:  "log field (date)",
	ConditionKindLogID:           "logId",
	ConditionKindLedger:          "ledger",
}

// allConditionKinds lists every real ConditionKind (excluding the Unknown
// sentinel). The completeness test asserts that every entry here has a name and
// an explicit per-target validity declaration, so a new condition kind cannot be
// added without declaring its validity.
var allConditionKinds = []ConditionKind{
	ConditionKindCombinator,
	ConditionKindField,
	ConditionKindAddress,
	ConditionKindReference,
	ConditionKindReverted,
	ConditionKindAccountHasAsset,
	ConditionKindBuiltinUint,
	ConditionKindLogBuiltinUint,
	ConditionKindLogID,
	ConditionKindLedger,
}

// String returns the human-readable label for the kind.
func (k ConditionKind) String() string {
	if name, ok := conditionKindNames[k]; ok {
		return name
	}

	return fmt.Sprintf("ConditionKind(%d)", int(k))
}

// ConditionKindOf maps a QueryFilter node to its ConditionKind. A nil filter or
// an unmapped arm returns ConditionKindUnknown, which always fails validation
// (invariant: a condition the validator does not understand is rejected, never
// silently admitted).
func ConditionKindOf(f *QueryFilter) ConditionKind {
	if f == nil {
		return ConditionKindUnknown
	}

	switch f.GetFilter().(type) {
	case *QueryFilter_And, *QueryFilter_Or, *QueryFilter_Not:
		return ConditionKindCombinator
	case *QueryFilter_Field:
		return ConditionKindField
	case *QueryFilter_Address:
		return ConditionKindAddress
	case *QueryFilter_Reference:
		return ConditionKindReference
	case *QueryFilter_Reverted:
		return ConditionKindReverted
	case *QueryFilter_AccountHasAsset:
		return ConditionKindAccountHasAsset
	case *QueryFilter_BuiltinUint:
		return ConditionKindBuiltinUint
	case *QueryFilter_LogBuiltinUint:
		return ConditionKindLogBuiltinUint
	case *QueryFilter_LogId:
		return ConditionKindLogID
	case *QueryFilter_Ledger:
		return ConditionKindLedger
	default:
		return ConditionKindUnknown
	}
}

// allQueryTargets lists every QueryTarget. The completeness test iterates it to
// assert that every (target, kind) pair is explicitly declared.
var allQueryTargets = []QueryTarget{
	QueryTarget_QUERY_TARGET_ACCOUNTS,
	QueryTarget_QUERY_TARGET_TRANSACTIONS,
	QueryTarget_QUERY_TARGET_LOGS,
}

// targetConditionValidity is the single authoritative table of which
// ConditionKind is valid on which QueryTarget. Both the compile layer and the
// REST decode layer consult it (via ConditionValidForTarget) so their rules
// cannot drift.
//
// The declarations reproduce the previously scattered guards exactly:
//   - combinator, metadata, address, reference: no target guard existed → valid
//     on every target (subject to the compiler's own index/schema checks).
//   - reverted, builtin uint (id/timestamp/insertedAt/revertedAt): TRANSACTIONS
//     only.
//   - accountHasAsset: ACCOUNTS only.
//   - log field (date), logId, ledger: LOGS only.
//
// Every (target, kind) pair MUST be present — a missing entry is a bug caught by
// the completeness test, not a silent "false". The map value is the explicit
// verdict for that pair.
var targetConditionValidity = map[QueryTarget]map[ConditionKind]bool{
	QueryTarget_QUERY_TARGET_ACCOUNTS: {
		ConditionKindCombinator:      true,
		ConditionKindField:           true,
		ConditionKindAddress:         true,
		ConditionKindReference:       true,
		ConditionKindReverted:        false,
		ConditionKindAccountHasAsset: true,
		ConditionKindBuiltinUint:     false,
		ConditionKindLogBuiltinUint:  false,
		ConditionKindLogID:           false,
		ConditionKindLedger:          false,
	},
	QueryTarget_QUERY_TARGET_TRANSACTIONS: {
		ConditionKindCombinator:      true,
		ConditionKindField:           true,
		ConditionKindAddress:         true,
		ConditionKindReference:       true,
		ConditionKindReverted:        true,
		ConditionKindAccountHasAsset: false,
		ConditionKindBuiltinUint:     true,
		ConditionKindLogBuiltinUint:  false,
		ConditionKindLogID:           false,
		ConditionKindLedger:          false,
	},
	QueryTarget_QUERY_TARGET_LOGS: {
		ConditionKindCombinator:      true,
		ConditionKindField:           true,
		ConditionKindAddress:         true,
		ConditionKindReference:       true,
		ConditionKindReverted:        false,
		ConditionKindAccountHasAsset: false,
		ConditionKindBuiltinUint:     false,
		ConditionKindLogBuiltinUint:  true,
		ConditionKindLogID:           true,
		ConditionKindLedger:          true,
	},
}

// ConditionValidForTarget reports whether the given ConditionKind is valid on
// the given QueryTarget, according to the single source of truth. An unknown
// kind (ConditionKindUnknown, or a kind with no declared entry) is never valid.
func ConditionValidForTarget(target QueryTarget, kind ConditionKind) bool {
	byKind, ok := targetConditionValidity[target]
	if !ok {
		return false
	}

	return byKind[kind]
}

// TargetHumanName returns a human-readable name for a query target, used in
// uniform validation error messages.
func TargetHumanName(target QueryTarget) string {
	switch target {
	case QueryTarget_QUERY_TARGET_TRANSACTIONS:
		return "transactions"
	case QueryTarget_QUERY_TARGET_LOGS:
		return "logs"
	case QueryTarget_QUERY_TARGET_ACCOUNTS:
		return "accounts"
	default:
		return fmt.Sprintf("QueryTarget(%d)", int(target))
	}
}
