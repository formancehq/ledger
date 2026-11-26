// Package indexes provides helpers around the canonical Index/IndexID protobuf
// types persisted in the bucket-scoped SubAttrIndex registry (keyed by
// {LedgerName, Canonical}). It is the single source of truth for identity
// comparison, canonical encoding, and lookup operations — all call sites
// (processor, FSM apply, indexbuilder, query compile, API handlers) should
// go through Find / Put / Remove rather than reaching into the oneof or
// the registry directly.
package indexes

import (
	"errors"
	"fmt"
	"strings"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// ErrInvalidCanonical is returned by ParseCanonical when the input string
// does not decode into a valid IndexID.
var ErrInvalidCanonical = errors.New("invalid canonical IndexID")

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

// SupportsMetadataTarget reports whether a metadata index can be built for the
// given target. Only ACCOUNT and TRANSACTION metadata have backfill paths
// (indexbuilder.addBackfillTaskForAcctMetadata / ...ForTxMetadata) and are
// queryable; a LEDGER-target metadata index would be registered but never
// backfilled, so it is not creatable. Callers validating a CreateIndex request
// (admission) gate on this so a broken, never-built index can never be
// persisted from any transport.
func SupportsMetadataTarget(target commonpb.TargetType) bool {
	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		return true
	default:
		return false
	}
}

// Supported reports whether the index builder has a backfill path for the
// given IndexID, i.e. whether a CreateIndex for it can ever complete rather
// than being persisted as permanently BUILDING. It is the single admission
// gate covering every IndexID kind:
//
//   - tx builtin: all named TransactionBuiltinIndex values have a backfill
//     path (reference/timestamp/id/address/source/destination/inserted_at/
//     reverted_at); an out-of-range enum is rejected.
//   - account builtin: only ACCT_BUILTIN_INDEX_ASSET is backfilled
//     (indexbuilder.backfill_postings); UNSPECIFIED and any other value are
//     rejected.
//   - log builtin: only LOG_BUILTIN_INDEX_DATE is backfilled; UNSPECIFIED and
//     any other value are rejected.
//   - metadata: gated by SupportsMetadataTarget (ACCOUNT/TRANSACTION only).
//
// ParseCanonical decodes any well-formed IndexID (including unsupported enum
// values reachable from an HTTP/gRPC create body), so admission must gate on
// this to keep a never-built index from being persisted from any transport.
func Supported(id *commonpb.IndexID) bool {
	switch k := id.GetKind().(type) {
	case *commonpb.IndexID_TxBuiltin:
		_, named := commonpb.TransactionBuiltinIndex_name[int32(k.TxBuiltin)]

		return named
	case *commonpb.IndexID_AccountBuiltin:
		return k.AccountBuiltin == commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET
	case *commonpb.IndexID_LogBuiltin:
		return k.LogBuiltin == commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE
	case *commonpb.IndexID_Metadata:
		return SupportsMetadataTarget(k.Metadata.GetTarget())
	default:
		return false
	}
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

// ParseCanonical is the inverse of Canonical: it decodes a string like
// "tx_builtin:TX_BUILTIN_INDEX_REFERENCE" or
// "metadata:TARGET_TYPE_ACCOUNT:color" back into an IndexID. Returns
// ErrInvalidCanonical when the prefix, enum value, or metadata target is
// unknown, or when the metadata key is empty.
func ParseCanonical(s string) (*commonpb.IndexID, error) {
	prefix, rest, ok := strings.Cut(s, ":")
	if !ok {
		return nil, fmt.Errorf("%w: missing prefix in %q", ErrInvalidCanonical, s)
	}

	switch prefix {
	case "tx_builtin":
		v, ok := commonpb.TransactionBuiltinIndex_value[rest]
		if !ok {
			return nil, fmt.Errorf("%w: unknown tx builtin %q", ErrInvalidCanonical, rest)
		}

		return TxBuiltinID(commonpb.TransactionBuiltinIndex(v)), nil

	case "log_builtin":
		v, ok := commonpb.LogBuiltinIndex_value[rest]
		if !ok {
			return nil, fmt.Errorf("%w: unknown log builtin %q", ErrInvalidCanonical, rest)
		}

		return LogBuiltinID(commonpb.LogBuiltinIndex(v)), nil

	case "account_builtin":
		v, ok := commonpb.AccountBuiltinIndex_value[rest]
		if !ok {
			return nil, fmt.Errorf("%w: unknown account builtin %q", ErrInvalidCanonical, rest)
		}

		return AccountBuiltinID(commonpb.AccountBuiltinIndex(v)), nil

	case "metadata":
		targetStr, key, ok := strings.Cut(rest, ":")
		if !ok || key == "" {
			return nil, fmt.Errorf("%w: metadata canonical must be metadata:<target>:<key>", ErrInvalidCanonical)
		}

		v, ok := commonpb.TargetType_value[targetStr]
		if !ok {
			return nil, fmt.Errorf("%w: unknown metadata target %q", ErrInvalidCanonical, targetStr)
		}

		return MetadataID(commonpb.TargetType(v), key), nil
	}

	return nil, fmt.Errorf("%w: unknown prefix %q", ErrInvalidCanonical, prefix)
}
