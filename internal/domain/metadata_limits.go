package domain

import (
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/pkg/metadatalimits"
)

// Metadata dimension identifiers and startup defaults are owned by the public contract.
const (
	MetadataLimitDimensionEntries      = metadatalimits.MetadataLimitDimensionEntries
	MetadataLimitDimensionKey          = metadatalimits.MetadataLimitDimensionKey
	MetadataLimitDimensionValue        = metadatalimits.MetadataLimitDimensionValue
	MetadataLimitDimensionEntity       = metadatalimits.MetadataLimitDimensionEntity
	MetadataLimitDimensionCommand      = metadatalimits.MetadataLimitDimensionCommand
	DefaultMetadataMaxEntriesPerEntity = metadatalimits.DefaultMetadataMaxEntriesPerEntity
	DefaultMetadataMaxKeyBytes         = metadatalimits.DefaultMetadataMaxKeyBytes
	DefaultMetadataMaxValueBytes       = metadatalimits.DefaultMetadataMaxValueBytes
	DefaultMetadataMaxEntityBytes      = metadatalimits.DefaultMetadataMaxEntityBytes
	DefaultMetadataMaxCommandBytes     = metadatalimits.DefaultMetadataMaxCommandBytes
)

// DefaultMetadataLimits supplies startup defaults, never a fallback for missing policy.
var DefaultMetadataLimits = metadatalimits.DefaultMetadataLimits

// MetadataLimitsFromPolicy reads the effective ceilings out of a committed
// cluster policy. A nil policy — or one predating the metadata ceilings —
// yields a zero metadatalimits.MetadataLimits, which Configured reports as unconfigured; the
// validators then reject loudly rather than admitting unbounded metadata.
func MetadataLimitsFromPolicy(policy *commonpb.ClusterPolicy) metadatalimits.MetadataLimits {
	return metadatalimits.MetadataLimits{
		MaxEntriesPerEntity:     policy.GetMetadataMaxEntriesPerEntity(),
		MaxKeyBytes:             policy.GetMetadataMaxKeyBytes(),
		MaxValueBytes:           policy.GetMetadataMaxValueBytes(),
		MaxTotalBytesPerEntity:  policy.GetMetadataMaxEntityBytes(),
		MaxTotalBytesPerCommand: policy.GetMetadataMaxCommandBytes(),
	}
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
		return metadatalimits.MetadataScalarValueBytes
	case *commonpb.MetadataValue_BoolValue:
		return metadatalimits.MetadataBoolValueBytes
	default:
		return 0
	}
}

// MetadataEntrySize adapts Ledger's metadata value to the shared semantic contract.
func MetadataEntrySize(key string, value *commonpb.MetadataValue) uint64 {
	return metadatalimits.MetadataEntrySize(key, MetadataValueSize(value))
}

// MetadataMapSize measures metadata through the shared semantic contract.
func MetadataMapSize(m map[string]*commonpb.MetadataValue) uint64 {
	return metadatalimits.MetadataMapSize(m, MetadataValueSize)
}

// ValidateMetadataMap preserves Ledger's typed audited failures at the public contract boundary.
func ValidateMetadataMap(limits metadatalimits.MetadataLimits, m map[string]*commonpb.MetadataValue) SerializableError {
	return metadataLimitFailure(metadatalimits.ValidateMap(limits, m, MetadataValueSize))
}

// ValidateMetadataLimitKey preserves typed failures for bare-key orders.
func ValidateMetadataLimitKey(limits metadatalimits.MetadataLimits, key string) SerializableError {
	return metadataLimitFailure(limits.ValidateKey(key))
}

// ValidateMetadataCommandBytes preserves typed failures for proposal-wide metadata.
func ValidateMetadataCommandBytes(limits metadatalimits.MetadataLimits, total uint64) SerializableError {
	return metadataLimitFailure(limits.ValidateCommandBytes(total))
}
func metadataLimitFailure(err error) SerializableError {
	switch e := err.(type) {
	case nil:
		return nil
	case *metadatalimits.ErrMetadataKeyValidation:
		return &ErrMetadataKeyValidation{Key: e.Key, Cause: metadataLimitFailure(e.Cause)}
	case *metadatalimits.ErrMetadataLimitExceeded:
		return &ErrMetadataLimitExceeded{Dimension: e.Dimension, Limit: e.Limit, Actual: e.Actual}
	default:
		if err == metadatalimits.ErrMetadataLimitsUnconfigured {
			return ErrMetadataLimitsUnconfigured
		}
		panic("unexpected metadata limit failure: " + err.Error())
	}
}
