package attributes

import (
	"github.com/holiman/uint256"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// Attributes holds all attribute types used in the ledger.
// Each instance has its own pre-allocated key buffer for thread-safe concurrent access.
type Attributes struct {
	Volume          *AccumulatingAttribute[*raftcmdpb.VolumePair]
	Metadata        *SimpleAttribute[*commonpb.MetadataValue]
	IdempotencyKeys *SimpleAttribute[*commonpb.IdempotencyKeyValue]
	References      *SimpleAttribute[*commonpb.TransactionReferenceValue]
	Ledger          *SimpleAttribute[*commonpb.LedgerInfo]
	Boundary        *SimpleAttribute[*raftcmdpb.LedgerBoundaries]
}

// New creates a new Attributes instance with all attribute types initialized.
func New() *Attributes {
	return &Attributes{
		Volume:          NewVolumeAttribute(),
		Metadata:        NewMetadataAttribute(),
		IdempotencyKeys: NewIdempotencyKeysAttribute(),
		References:      NewReferenceAttribute(),
		Ledger:          NewLedgerAttribute(),
		Boundary:        NewBoundaryAttribute(),
	}
}

// NewVolumeAttribute creates a new Volume attribute storing merged Input+Output pairs.
func NewVolumeAttribute() *AccumulatingAttribute[*raftcmdpb.VolumePair] {
	return &AccumulatingAttribute[*raftcmdpb.VolumePair]{core: core[*raftcmdpb.VolumePair]{
		prefix:   dal.AttributePrefixVolume,
		newValue: func() *raftcmdpb.VolumePair { return &raftcmdpb.VolumePair{} },
		resolveFn: func(base *raftcmdpb.VolumePair, lastDiff *raftcmdpb.VolumePair) *raftcmdpb.VolumePair {
			var inputResult, outputResult, tmp uint256.Int

			if base != nil {
				if base.GetInputKnown() != nil {
					base.GetInputKnown().IntoUint256(&inputResult)
				}

				if base.GetOutputKnown() != nil {
					base.GetOutputKnown().IntoUint256(&outputResult)
				}
			}

			if lastDiff != nil {
				if lastDiff.GetInputKnown() != nil {
					lastDiff.GetInputKnown().IntoUint256(&tmp)
					inputResult.Add(&inputResult, &tmp)
				}

				if lastDiff.GetOutputKnown() != nil {
					lastDiff.GetOutputKnown().IntoUint256(&tmp)
					outputResult.Add(&outputResult, &tmp)
				}
			}

			return &raftcmdpb.VolumePair{
				InputKnown:  commonpb.NewUint256(&inputResult),
				OutputKnown: commonpb.NewUint256(&outputResult),
			}
		},
		keyBuf: make([]byte, 128),
	}}
}

// simpleResolve returns the base value, ignoring diffs. Used for all SimpleAttribute types.
func simpleResolve[V proto.Message](base, _ V) V {
	return base
}

// NewMetadataAttribute creates a new Metadata attribute for account metadata.
func NewMetadataAttribute() *SimpleAttribute[*commonpb.MetadataValue] {
	return &SimpleAttribute[*commonpb.MetadataValue]{core: core[*commonpb.MetadataValue]{
		prefix:    dal.AttributePrefixMetadata,
		newValue:  func() *commonpb.MetadataValue { return &commonpb.MetadataValue{} },
		resolveFn: simpleResolve[*commonpb.MetadataValue],
		keyBuf:    make([]byte, 128),
	}}
}

// NewIdempotencyKeysAttribute creates a new IdempotencyKeys attribute for storing idempotency key mappings.
func NewIdempotencyKeysAttribute() *SimpleAttribute[*commonpb.IdempotencyKeyValue] {
	return &SimpleAttribute[*commonpb.IdempotencyKeyValue]{core: core[*commonpb.IdempotencyKeyValue]{
		prefix:    dal.AttributePrefixIdempotency,
		newValue:  func() *commonpb.IdempotencyKeyValue { return &commonpb.IdempotencyKeyValue{} },
		resolveFn: simpleResolve[*commonpb.IdempotencyKeyValue],
		keyBuf:    make([]byte, 128),
	}}
}

// NewReferenceAttribute creates a new Reference attribute for storing transaction reference mappings.
func NewReferenceAttribute() *SimpleAttribute[*commonpb.TransactionReferenceValue] {
	return &SimpleAttribute[*commonpb.TransactionReferenceValue]{core: core[*commonpb.TransactionReferenceValue]{
		prefix:    dal.AttributePrefixReference,
		newValue:  func() *commonpb.TransactionReferenceValue { return &commonpb.TransactionReferenceValue{} },
		resolveFn: simpleResolve[*commonpb.TransactionReferenceValue],
		keyBuf:    make([]byte, 128),
	}}
}

// NewLedgerAttribute creates a new Ledger attribute for storing ledger info.
func NewLedgerAttribute() *SimpleAttribute[*commonpb.LedgerInfo] {
	return &SimpleAttribute[*commonpb.LedgerInfo]{core: core[*commonpb.LedgerInfo]{
		prefix:    dal.AttributePrefixLedger,
		newValue:  func() *commonpb.LedgerInfo { return &commonpb.LedgerInfo{} },
		resolveFn: simpleResolve[*commonpb.LedgerInfo],
		keyBuf:    make([]byte, 128),
	}}
}

// NewBoundaryAttribute creates a new Boundary attribute for storing ledger boundaries.
func NewBoundaryAttribute() *SimpleAttribute[*raftcmdpb.LedgerBoundaries] {
	return &SimpleAttribute[*raftcmdpb.LedgerBoundaries]{core: core[*raftcmdpb.LedgerBoundaries]{
		prefix:    dal.AttributePrefixBoundary,
		newValue:  func() *raftcmdpb.LedgerBoundaries { return &raftcmdpb.LedgerBoundaries{} },
		resolveFn: simpleResolve[*raftcmdpb.LedgerBoundaries],
		keyBuf:    make([]byte, 128),
	}}
}

// Module returns the fx module for the attributes package.
func Module() fx.Option {
	return fx.Provide(New)
}
