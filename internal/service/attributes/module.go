package attributes

import (
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"go.uber.org/fx"
)

// Attributes holds all attribute types used in the ledger.
// Each instance has its own pre-allocated key buffer for thread-safe concurrent access.
type Attributes struct {
	Volume          *Attribute[*raftcmdpb.VolumePair]
	Metadata        *Attribute[*commonpb.MetadataValue]
	LedgerMetadata  *Attribute[*commonpb.MetadataValue]
	Reverted        *Attribute[*commonpb.RevertedValue]
	IdempotencyKeys *Attribute[*commonpb.IdempotencyKeyValue]
	References      *Attribute[*commonpb.TransactionReferenceValue]
	Ledger          *Attribute[*commonpb.LedgerInfo]
	Boundary        *Attribute[*raftcmdpb.LedgerBoundaries]
}

// New creates a new Attributes instance with all attribute types initialized.
func New() *Attributes {
	return &Attributes{
		Volume:          NewVolumeAttribute(),
		Metadata:        NewMetadataAttribute(),
		LedgerMetadata:  NewLedgerMetadataAttribute(),
		Reverted:        NewRevertedAttribute(),
		IdempotencyKeys: NewIdempotencyKeysAttribute(),
		References:      NewReferenceAttribute(),
		Ledger:          NewLedgerAttribute(),
		Boundary:        NewBoundaryAttribute(),
	}
}

// NewVolumeAttribute creates a new Volume attribute storing merged Input+Output pairs.
func NewVolumeAttribute() *Attribute[*raftcmdpb.VolumePair] {
	return &Attribute[*raftcmdpb.VolumePair]{
		prefix:   data.AttributePrefixVolume,
		newValue: func() *raftcmdpb.VolumePair { return &raftcmdpb.VolumePair{} },
		computeFn: func(base *raftcmdpb.VolumePair, lastDiff *raftcmdpb.VolumePair) *raftcmdpb.VolumePair {
			inputResult := big.NewInt(0)
			outputResult := big.NewInt(0)

			if base != nil {
				if base.InputKnown != nil {
					inputResult = base.InputKnown.Value()
				}
				if base.OutputKnown != nil {
					outputResult = base.OutputKnown.Value()
				}
			}
			if lastDiff != nil {
				if lastDiff.InputKnown != nil {
					inputResult = new(big.Int).Add(inputResult, lastDiff.InputKnown.Value())
				}
				if lastDiff.OutputKnown != nil {
					outputResult = new(big.Int).Add(outputResult, lastDiff.OutputKnown.Value())
				}
			}
			return &raftcmdpb.VolumePair{
				InputKnown:  commonpb.NewBigInt(inputResult),
				OutputKnown: commonpb.NewBigInt(outputResult),
			}
		},
		keyBuf: make([]byte, 128),
	}
}

// NewMetadataAttribute creates a new Metadata attribute for account metadata.
func NewMetadataAttribute() *Attribute[*commonpb.MetadataValue] {
	return &Attribute[*commonpb.MetadataValue]{
		prefix:   data.AttributePrefixMetadata,
		newValue: func() *commonpb.MetadataValue { return &commonpb.MetadataValue{} },
		computeFn: func(base *commonpb.MetadataValue, lastDiff *commonpb.MetadataValue) *commonpb.MetadataValue {
			if lastDiff == nil {
				return base
			}
			return lastDiff
		},
		keyBuf: make([]byte, 128),
	}
}

// NewLedgerMetadataAttribute creates a new LedgerMetadata attribute for ledger metadata.
func NewLedgerMetadataAttribute() *Attribute[*commonpb.MetadataValue] {
	return &Attribute[*commonpb.MetadataValue]{
		prefix:   data.AttributePrefixLedgerMetadata,
		newValue: func() *commonpb.MetadataValue { return &commonpb.MetadataValue{} },
		computeFn: func(base *commonpb.MetadataValue, lastDiff *commonpb.MetadataValue) *commonpb.MetadataValue {
			if lastDiff == nil {
				return base
			}
			return lastDiff
		},
		keyBuf: make([]byte, 128),
	}
}

// NewRevertedAttribute creates a new Reverted attribute for tracking transaction reversion status.
func NewRevertedAttribute() *Attribute[*commonpb.RevertedValue] {
	return &Attribute[*commonpb.RevertedValue]{
		prefix:   data.AttributePrefixReverted,
		newValue: func() *commonpb.RevertedValue { return &commonpb.RevertedValue{} },
		computeFn: func(base *commonpb.RevertedValue, lastDiff *commonpb.RevertedValue) *commonpb.RevertedValue {
			if lastDiff == nil {
				if base == nil {
					return &commonpb.RevertedValue{Reverted: false}
				}
				return base
			}
			return lastDiff
		},
		keyBuf: make([]byte, 128),
	}
}

// NewIdempotencyKeysAttribute creates a new IdempotencyKeys attribute for storing idempotency key mappings.
func NewIdempotencyKeysAttribute() *Attribute[*commonpb.IdempotencyKeyValue] {
	return &Attribute[*commonpb.IdempotencyKeyValue]{
		prefix:   data.AttributePrefixIdempotencyKey,
		newValue: func() *commonpb.IdempotencyKeyValue { return &commonpb.IdempotencyKeyValue{} },
		computeFn: func(base *commonpb.IdempotencyKeyValue, lastDiff *commonpb.IdempotencyKeyValue) *commonpb.IdempotencyKeyValue {
			if base != nil {
				return base
			}
			return lastDiff
		},
		keyBuf: make([]byte, 128),
	}
}

// NewReferenceAttribute creates a new Reference attribute for storing transaction reference mappings.
func NewReferenceAttribute() *Attribute[*commonpb.TransactionReferenceValue] {
	return &Attribute[*commonpb.TransactionReferenceValue]{
		prefix:   data.AttributePrefixReference,
		newValue: func() *commonpb.TransactionReferenceValue { return &commonpb.TransactionReferenceValue{} },
		computeFn: func(base *commonpb.TransactionReferenceValue, lastDiff *commonpb.TransactionReferenceValue) *commonpb.TransactionReferenceValue {
			if base != nil {
				return base
			}
			return lastDiff
		},
		keyBuf: make([]byte, 128),
	}
}

// NewLedgerAttribute creates a new Ledger attribute for storing ledger info.
func NewLedgerAttribute() *Attribute[*commonpb.LedgerInfo] {
	return &Attribute[*commonpb.LedgerInfo]{
		prefix:   data.AttributePrefixLedger,
		newValue: func() *commonpb.LedgerInfo { return &commonpb.LedgerInfo{} },
		computeFn: func(base *commonpb.LedgerInfo, lastDiff *commonpb.LedgerInfo) *commonpb.LedgerInfo {
			if lastDiff == nil {
				return base
			}
			return lastDiff
		},
		keyBuf: make([]byte, 128),
	}
}

// NewBoundaryAttribute creates a new Boundary attribute for storing ledger boundaries.
func NewBoundaryAttribute() *Attribute[*raftcmdpb.LedgerBoundaries] {
	return &Attribute[*raftcmdpb.LedgerBoundaries]{
		prefix:   data.AttributePrefixBoundary,
		newValue: func() *raftcmdpb.LedgerBoundaries { return &raftcmdpb.LedgerBoundaries{} },
		computeFn: func(base *raftcmdpb.LedgerBoundaries, lastDiff *raftcmdpb.LedgerBoundaries) *raftcmdpb.LedgerBoundaries {
			if lastDiff == nil {
				return base
			}
			return lastDiff
		},
		keyBuf: make([]byte, 128),
	}
}

// Module returns the fx module for the attributes package.
func Module() fx.Option {
	return fx.Provide(New)
}
