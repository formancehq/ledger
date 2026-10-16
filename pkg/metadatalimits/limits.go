package metadatalimits

import (
	"errors"
	"fmt"
)

// Metadata limit dimension names. They are carried in
// Ledger's typed failure details and reach the client through
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
// a document; 64 KiB per entity and 1 MiB per command bound what one accepted
// write can replicate through Raft into the audit chain, the FSM cache, and the
// read-side projections.
//
// The command ceiling was raised from 256 KiB to 1 MiB (EN-2069/EN-2105):
// 256 KiB only allowed 4 entities at the 64 KiB per-entity maximum, which
// connector workloads routinely exceeded in valid bulk commands. 1 MiB allows
// up to 16 entities at their per-entity maximum, restoring meaningful fan-out
// headroom without removing the bound.
const (
	DefaultMetadataMaxEntriesPerEntity = 128
	DefaultMetadataMaxKeyBytes         = 256
	DefaultMetadataMaxValueBytes       = 16 << 10
	DefaultMetadataMaxEntityBytes      = 64 << 10
	DefaultMetadataMaxCommandBytes     = 1 << 20
)

// Measured widths of the non-string MetadataValue variants. The scalar variants
// (int, uint, datetime micros) are 64-bit and the bool variant is one byte.
// These are accounting weights for the size contract, not wire sizes: a fixed
// weight per variant keeps MetadataEntrySize a pure function of the value,
// which is what makes the FSM-side check deterministic across nodes and stable
// across protobuf encoding changes.
const (
	MetadataScalarValueBytes = 8
	MetadataBoolValueBytes   = 1
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
// Ledger reads the effective values from its replicated cluster policy, so
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

// Validate checks that the ceilings form a usable policy. Bootstrap and FSM
// policy updates share this validation so their acceptance rules cannot drift.
func (l MetadataLimits) Validate() error {
	if !l.Configured() {
		return errors.New("every metadata_max_* limit must be at least 1")
	}
	if !l.Consistent() {
		return errors.New("metadata limits must satisfy metadata_max_key_bytes <= metadata_max_entity_bytes, " +
			"metadata_max_value_bytes <= metadata_max_entity_bytes and " +
			"metadata_max_entity_bytes <= metadata_max_command_bytes")
	}

	return nil
}

// MetadataEntrySize is the measured size of one key/value entry: the key's
// UTF-8 bytes plus the value's measured bytes. This is the single measurement
// rule — admission and FSM apply both call it, so the two layers can never
// disagree about whether the same payload fits.
func MetadataEntrySize(key string, valueBytes uint64) uint64 {
	return uint64(len(key)) + valueBytes
}

// MetadataMapSize is the measured size of a whole metadata map. Summation is
// order-independent, so the result is identical on every node regardless of Go
// map iteration order. The valueSize function must be pure and follow Ledger's
// semantic weights: strings and null original text use UTF-8 byte length,
// integer/unsigned/datetime values use MetadataScalarValueBytes, booleans use
// MetadataBoolValueBytes, and absent/unknown values use zero. It must handle
// nil values when V permits them. This function does not validate value shape.
func MetadataMapSize[V any](m map[string]V, valueSize func(V) uint64) uint64 {
	var total uint64
	for key, value := range m {
		total += MetadataEntrySize(key, valueSize(value))
	}

	return total
}

// ValidateMap checks one entity's metadata map against the contract: entry
// count, per-key size, per-value size, and the per-entity total. valueSize has
// the same contract as MetadataMapSize. Shape validation remains separate.
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
func ValidateMap[V any](l MetadataLimits, m map[string]V, valueSize func(V) uint64) error {
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

	if total := MetadataMapSize(m, valueSize); total > l.MaxTotalBytesPerEntity {
		return &ErrMetadataLimitExceeded{
			Dimension: MetadataLimitDimensionEntity,
			Limit:     l.MaxTotalBytesPerEntity,
			Actual:    total,
		}
	}

	var (
		worstKey string
		worstErr error
	)

	for key, value := range m {
		err := l.ValidateEntry(key, valueSize(value))
		if err == nil {
			continue
		}

		if worstErr == nil || key < worstKey {
			worstKey, worstErr = key, err
		}
	}

	if worstErr != nil {
		if _, keyed := worstErr.(*ErrMetadataKeyValidation); keyed {
			return worstErr
		}

		return &ErrMetadataKeyValidation{Key: worstKey, Cause: worstErr}
	}

	return nil
}

// ValidateKey checks one metadata key against the per-key ceiling. Exported for
// the orders that carry a bare key and no value (delete-metadata,
// set/remove-metadata-field-type): they must be bounded by the same rule as a
// key inside a map, or an oversized key could enter the canonical Pebble key
// layout through the delete path.
func (l MetadataLimits) ValidateKey(key string) error {
	if !l.Configured() {
		return ErrMetadataLimitsUnconfigured
	}

	if keyBytes := uint64(len(key)); keyBytes > l.MaxKeyBytes {
		return &ErrMetadataKeyValidation{Key: key, Cause: &ErrMetadataLimitExceeded{
			Dimension: MetadataLimitDimensionKey,
			Limit:     l.MaxKeyBytes,
			Actual:    keyBytes,
		}}
	}

	return nil
}

// ValidateEntry checks one entry's key and value sizes. The key is checked
// before the value so an entry violating both reports the key deterministically.
func (l MetadataLimits) ValidateEntry(key string, valueBytes uint64) error {
	if err := l.ValidateKey(key); err != nil {
		return err
	}

	if valueBytes > l.MaxValueBytes {
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
func (l MetadataLimits) ValidateCommandBytes(total uint64) error {
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

// ErrMetadataLimitsUnconfigured means ceilings are missing, never unlimited.
var ErrMetadataLimitsUnconfigured = errors.New("the committed cluster policy carries no metadata size limits; bump --cluster-policy-revision to commit one")

// ErrMetadataLimitExceeded identifies the measured dimension and ceiling.
type ErrMetadataLimitExceeded struct {
	Dimension string
	Limit     uint64
	Actual    uint64
}

func (e *ErrMetadataLimitExceeded) Error() string {
	if e.Dimension == MetadataLimitDimensionEntries {
		return fmt.Sprintf("metadata carries %d entries, limit is %d per entity", e.Actual, e.Limit)
	}
	return fmt.Sprintf("metadata %s size is %d bytes, limit is %d", e.Dimension, e.Actual, e.Limit)
}

// ErrMetadataKeyValidation attaches the offending key to a size failure.
type ErrMetadataKeyValidation struct {
	Key   string
	Cause error
}

func (e *ErrMetadataKeyValidation) Error() string {
	return fmt.Sprintf("metadata key %q value: %s", e.Key, e.Cause.Error())
}
func (e *ErrMetadataKeyValidation) Unwrap() error { return e.Cause }
