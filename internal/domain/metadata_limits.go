package domain

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// Metadata limit dimension names. They are carried in
// ErrMetadataLimitExceeded.Metadata()["dimension"] and reach the client through
// the gRPC ErrorInfo, so a caller can tell "too many entries" from "one value
// too large" without parsing the message. Stable wire identifiers: renaming one
// is a client-visible change.
const (
	MetadataLimitDimensionEntries = "entries"
	MetadataLimitDimensionKey     = "key"
	MetadataLimitDimensionValue   = "value"
	MetadataLimitDimensionEntity  = "entity"
	MetadataLimitDimensionCommand = "command"
)

// Default metadata ceilings (EN-1829). They are the safe defaults the server
// flags carry into the replicated cluster policy, not a fallback applied when
// configuration is missing: an unconfigured policy fails loudly instead
// (ErrMetadataLimitsUnconfigured) so a misconfiguration can never silently
// disable the protection.
//
// 128 entries with 256-byte keys leaves a metadata map an operator can still
// inspect; 16 KiB per value admits a JSON blob or a signature without admitting
// a document; 64 KiB per entity and 256 KiB per command bound what one accepted
// write can replicate through Raft into the audit chain, the FSM cache, and the
// read-side projections.
const (
	DefaultMetadataMaxEntriesPerEntity = 128
	DefaultMetadataMaxKeyBytes         = 256
	DefaultMetadataMaxValueBytes       = 16 << 10
	DefaultMetadataMaxEntityBytes      = 64 << 10
	DefaultMetadataMaxCommandBytes     = 256 << 10
)

// Measured widths of the non-string MetadataValue variants. The scalar variants
// (int, uint, datetime micros) are 64-bit and the bool variant is one byte.
// These are accounting weights for the size contract, not wire sizes: a fixed
// weight per variant keeps MetadataEntrySize a pure function of the value,
// which is what makes the FSM-side check deterministic across nodes and stable
// across protobuf encoding changes.
const (
	metadataScalarValueBytes = 8
	metadataBoolValueBytes   = 1
)

// MetadataLimits is the canonical metadata size contract. Every ceiling is
// scoped to a single command: MaxEntriesPerEntity and MaxTotalBytesPerEntity
// bound the metadata one command carries for (or produces on) one entity, and
// MaxTotalBytesPerCommand bounds the whole command.
//
// The ceilings deliberately do NOT bound an entity's accumulated stored
// metadata. Enforcing that would require reading the entity's whole metadata
// namespace during apply — which no proposal's declared coverage authorises
// (invariant #6) — or a new persisted per-entity counter, which would be a
// primary-store projection needing checker verification (invariant #8) and a
// restore classification (invariant #11). See
// docs/technical/architecture/subsystems/admission/metadata-limits.md.
//
// The effective values live in the Raft-replicated commonpb.ClusterPolicy, so
// admission and FSM apply read the same committed numbers on every node
// (invariant #2).
type MetadataLimits struct {
	MaxEntriesPerEntity     uint64
	MaxKeyBytes             uint64
	MaxValueBytes           uint64
	MaxTotalBytesPerEntity  uint64
	MaxTotalBytesPerCommand uint64
}

// DefaultMetadataLimits is the contract with the default ceilings.
var DefaultMetadataLimits = MetadataLimits{
	MaxEntriesPerEntity:     DefaultMetadataMaxEntriesPerEntity,
	MaxKeyBytes:             DefaultMetadataMaxKeyBytes,
	MaxValueBytes:           DefaultMetadataMaxValueBytes,
	MaxTotalBytesPerEntity:  DefaultMetadataMaxEntityBytes,
	MaxTotalBytesPerCommand: DefaultMetadataMaxCommandBytes,
}

// MetadataLimitsFromPolicy reads the effective ceilings out of a committed
// cluster policy. A nil policy — or one predating the metadata ceilings —
// yields a zero MetadataLimits, which Configured reports as unconfigured; the
// validators then reject loudly rather than admitting unbounded metadata.
func MetadataLimitsFromPolicy(policy *commonpb.ClusterPolicy) MetadataLimits {
	return MetadataLimits{
		MaxEntriesPerEntity:     policy.GetMetadataMaxEntriesPerEntity(),
		MaxKeyBytes:             policy.GetMetadataMaxKeyBytes(),
		MaxValueBytes:           policy.GetMetadataMaxValueBytes(),
		MaxTotalBytesPerEntity:  policy.GetMetadataMaxEntityBytes(),
		MaxTotalBytesPerCommand: policy.GetMetadataMaxCommandBytes(),
	}
}

// Configured reports whether every ceiling carries a value. Zero is not "no
// limit": it is the absence of a committed configuration, and the validators
// treat it as a policy error.
func (l MetadataLimits) Configured() bool {
	return l.MaxEntriesPerEntity > 0 &&
		l.MaxKeyBytes > 0 &&
		l.MaxValueBytes > 0 &&
		l.MaxTotalBytesPerEntity > 0 &&
		l.MaxTotalBytesPerCommand > 0
}

// Consistent reports whether the ceilings can all be satisfied at once. A
// per-value ceiling above the per-entity one (or a per-entity ceiling above the
// per-command one) is a configuration mistake: the wider bound is unreachable,
// so an operator raising it would see no effect.
func (l MetadataLimits) Consistent() bool {
	return l.MaxKeyBytes <= l.MaxTotalBytesPerEntity &&
		l.MaxValueBytes <= l.MaxTotalBytesPerEntity &&
		l.MaxTotalBytesPerEntity <= l.MaxTotalBytesPerCommand
}

// MetadataValueSize is the measured size of one metadata value. Unknown and nil
// variants measure zero: an absent value carries no payload, and rejecting it
// is the shape validators' job (ValidateMetadataValue), not the size
// contract's.
func MetadataValueSize(value *commonpb.MetadataValue) uint64 {
	switch v := value.GetType().(type) {
	case *commonpb.MetadataValue_StringValue:
		return uint64(len(v.StringValue))
	case *commonpb.MetadataValue_NullValue:
		// A null value still carries the original text it failed to coerce.
		return uint64(len(v.NullValue.GetOriginal()))
	case *commonpb.MetadataValue_IntValue,
		*commonpb.MetadataValue_UintValue,
		*commonpb.MetadataValue_DatetimeValue:
		return metadataScalarValueBytes
	case *commonpb.MetadataValue_BoolValue:
		return metadataBoolValueBytes
	default:
		return 0
	}
}

// MetadataEntrySize is the measured size of one key/value entry: the key's
// UTF-8 bytes plus the value's measured bytes. This is the single measurement
// rule — admission and FSM apply both call it, so the two layers can never
// disagree about whether the same payload fits.
func MetadataEntrySize(key string, value *commonpb.MetadataValue) uint64 {
	return uint64(len(key)) + MetadataValueSize(value)
}

// MetadataMapSize is the measured size of a whole metadata map. Summation is
// order-independent, so the result is identical on every node regardless of Go
// map iteration order.
func MetadataMapSize(m map[string]*commonpb.MetadataValue) uint64 {
	var total uint64
	for key, value := range m {
		total += MetadataEntrySize(key, value)
	}

	return total
}

// ValidateMap checks one entity's metadata map against the contract: entry
// count, per-key size, per-value size, and the per-entity total.
//
// The checks run in a fixed order — count, then entity total, then per-entry —
// and the per-entry pass reports the lexicographically smallest offending key
// rather than the first one iteration happens to reach. Both properties matter
// on the FSM path: Go map iteration order is randomised, so an
// iteration-dependent choice of error would make one committed entry reject
// with different messages on different nodes, and the rejection is recorded in
// the audit chain (invariant #2).
//
// Per-entry failures are wrapped in ErrMetadataKeyValidation so the offending
// key reaches operator logs and the gRPC ErrorInfo, matching the shape
// validators.
func (l MetadataLimits) ValidateMap(m map[string]*commonpb.MetadataValue) Describable {
	if !l.Configured() {
		return ErrMetadataLimitsUnconfigured
	}

	if count := uint64(len(m)); count > l.MaxEntriesPerEntity {
		return &ErrMetadataLimitExceeded{
			Dimension: MetadataLimitDimensionEntries,
			Limit:     l.MaxEntriesPerEntity,
			Actual:    count,
		}
	}

	if total := MetadataMapSize(m); total > l.MaxTotalBytesPerEntity {
		return &ErrMetadataLimitExceeded{
			Dimension: MetadataLimitDimensionEntity,
			Limit:     l.MaxTotalBytesPerEntity,
			Actual:    total,
		}
	}

	var (
		worstKey string
		worstErr Describable
	)

	for key, value := range m {
		err := l.validateEntry(key, value)
		if err == nil {
			continue
		}

		if worstErr == nil || key < worstKey {
			worstKey, worstErr = key, err
		}
	}

	if worstErr != nil {
		return &ErrMetadataKeyValidation{Key: worstKey, Cause: worstErr}
	}

	return nil
}

// ValidateKey checks one metadata key against the per-key ceiling. Exported for
// the orders that carry a bare key and no value (delete-metadata,
// set/remove-metadata-field-type): they must be bounded by the same rule as a
// key inside a map, or an oversized key could enter the canonical Pebble key
// layout through the delete path.
func (l MetadataLimits) ValidateKey(key string) Describable {
	if !l.Configured() {
		return ErrMetadataLimitsUnconfigured
	}

	if keyBytes := uint64(len(key)); keyBytes > l.MaxKeyBytes {
		return &ErrMetadataLimitExceeded{
			Dimension: MetadataLimitDimensionKey,
			Limit:     l.MaxKeyBytes,
			Actual:    keyBytes,
		}
	}

	return nil
}

// validateEntry checks one entry's key and value sizes. The key is checked
// before the value so an entry violating both reports the key deterministically.
func (l MetadataLimits) validateEntry(key string, value *commonpb.MetadataValue) Describable {
	if err := l.ValidateKey(key); err != nil {
		return err
	}

	if valueBytes := MetadataValueSize(value); valueBytes > l.MaxValueBytes {
		return &ErrMetadataLimitExceeded{
			Dimension: MetadataLimitDimensionValue,
			Limit:     l.MaxValueBytes,
			Actual:    valueBytes,
		}
	}

	return nil
}

// ValidateCommandBytes checks the metadata total accumulated across every
// entity a single command touches. Admission sums the orders of one batch — the
// atomic, signed unit that becomes one Raft proposal — so a caller cannot
// defeat the per-entity ceiling by spreading a large payload over many entities
// in one command.
func (l MetadataLimits) ValidateCommandBytes(total uint64) Describable {
	if !l.Configured() {
		return ErrMetadataLimitsUnconfigured
	}

	if total > l.MaxTotalBytesPerCommand {
		return &ErrMetadataLimitExceeded{
			Dimension: MetadataLimitDimensionCommand,
			Limit:     l.MaxTotalBytesPerCommand,
			Actual:    total,
		}
	}

	return nil
}
