// Package indexes provides helpers around the canonical Index/IndexID protobuf
// types persisted in the bucket-scoped SubAttrIndex registry (keyed by
// {LedgerName, Canonical}). It is the single source of truth for identity
// comparison, canonical encoding, and lookup operations — all call sites
// (processor, FSM apply, indexbuilder, query compile, API handlers) should
// go through Find / Put / Remove rather than reaching into the oneof or
// the registry directly.
package indexes

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TxBuiltinID builds an IndexID for a transaction builtin field.
func TxBuiltinID(kind commonpb.TransactionBuiltinIndex) *commonpb.IndexID {
	return &commonpb.IndexID{Kind: &commonpb.IndexID_TxBuiltin{TxBuiltin: kind}}
}

// LogBuiltinID builds an IndexID for a log builtin field.
func LogBuiltinID(kind commonpb.LogBuiltinIndex) *commonpb.IndexID {
	return &commonpb.IndexID{Kind: &commonpb.IndexID_LogBuiltin{LogBuiltin: kind}}
}

// AccountBuiltinID builds an IndexID for an account builtin field.
func AccountBuiltinID(kind commonpb.AccountBuiltinIndex) *commonpb.IndexID {
	return &commonpb.IndexID{Kind: &commonpb.IndexID_AccountBuiltin{AccountBuiltin: kind}}
}

// MetadataID builds an IndexID for a metadata-key index on the given target.
func MetadataID(target commonpb.TargetType, key string) *commonpb.IndexID {
	return &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
		Target: target,
		Key:    key,
	}}}
}

// Equal returns true iff a and b designate the same logical index. Nil-safe.
func Equal(a, b *commonpb.IndexID) bool {
	if a == nil || b == nil {
		return a == b
	}

	switch ka := a.GetKind().(type) {
	case *commonpb.IndexID_TxBuiltin:
		kb, ok := b.GetKind().(*commonpb.IndexID_TxBuiltin)

		return ok && ka.TxBuiltin == kb.TxBuiltin
	case *commonpb.IndexID_LogBuiltin:
		kb, ok := b.GetKind().(*commonpb.IndexID_LogBuiltin)

		return ok && ka.LogBuiltin == kb.LogBuiltin
	case *commonpb.IndexID_AccountBuiltin:
		kb, ok := b.GetKind().(*commonpb.IndexID_AccountBuiltin)

		return ok && ka.AccountBuiltin == kb.AccountBuiltin
	case *commonpb.IndexID_Metadata:
		kb, ok := b.GetKind().(*commonpb.IndexID_Metadata)
		if !ok {
			return false
		}

		return ka.Metadata.GetTarget() == kb.Metadata.GetTarget() &&
			ka.Metadata.GetKey() == kb.Metadata.GetKey()
	}

	return false
}

// Canonical returns a stable string representation of an IndexID, suitable for
// logs, map keys, and dedup. Format: "<prefix>:<value>".
func Canonical(id *commonpb.IndexID) string {
	if id == nil {
		return ""
	}

	switch k := id.GetKind().(type) {
	case *commonpb.IndexID_TxBuiltin:
		return "tx_builtin:" + k.TxBuiltin.String()
	case *commonpb.IndexID_LogBuiltin:
		return "log_builtin:" + k.LogBuiltin.String()
	case *commonpb.IndexID_AccountBuiltin:
		return "account_builtin:" + k.AccountBuiltin.String()
	case *commonpb.IndexID_Metadata:
		return fmt.Sprintf("metadata:%s:%s", k.Metadata.GetTarget().String(), k.Metadata.GetKey())
	}

	return "unknown"
}
