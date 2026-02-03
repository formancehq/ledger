package attributes

import (
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"go.uber.org/fx"
)

// Attributes holds all attribute types used in the ledger.
// Each instance has its own KeyBuilder for thread-safe concurrent access.
type Attributes struct {
	Input           *Attribute[*commonpb.BigInt]
	Output          *Attribute[*commonpb.BigInt]
	Metadata        *Attribute[*commonpb.MetadataValue]
	LedgerMetadata  *Attribute[*commonpb.MetadataValue]
	Reverted        *Attribute[*commonpb.RevertedValue]
	IdempotencyKeys *Attribute[*commonpb.IdempotencyKeyValue]
}

// New creates a new Attributes instance with all attribute types initialized.
func New() *Attributes {
	return &Attributes{
		Input:           NewInputAttribute(),
		Output:          NewOutputAttribute(),
		Metadata:        NewMetadataAttribute(),
		LedgerMetadata:  NewLedgerMetadataAttribute(),
		Reverted:        NewRevertedAttribute(),
		IdempotencyKeys: NewIdempotencyKeysAttribute(),
	}
}

// NewInputAttribute creates a new Input attribute for account fund inputs (credits).
func NewInputAttribute() *Attribute[*commonpb.BigInt] {
	return &Attribute[*commonpb.BigInt]{
		prefix:   data.AttributePrefixInput,
		newValue: func() *commonpb.BigInt { return &commonpb.BigInt{} },
		computeFn: func(base *commonpb.BigInt, diffs []*commonpb.BigInt) *commonpb.BigInt {
			result := big.NewInt(0)
			if base != nil {
				result = base.Value()
			}
			for _, diff := range diffs {
				result = new(big.Int).Add(result, diff.Value())
			}
			return commonpb.NewBigInt(result)
		},
		kb: data.NewKeyBuilder(),
	}
}

// NewOutputAttribute creates a new Output attribute for account fund outputs (debits).
func NewOutputAttribute() *Attribute[*commonpb.BigInt] {
	return &Attribute[*commonpb.BigInt]{
		prefix:   data.AttributePrefixOutput,
		newValue: func() *commonpb.BigInt { return &commonpb.BigInt{} },
		computeFn: func(base *commonpb.BigInt, diffs []*commonpb.BigInt) *commonpb.BigInt {
			result := big.NewInt(0)
			if base != nil {
				result = base.Value()
			}
			for _, diff := range diffs {
				result = new(big.Int).Add(result, diff.Value())
			}
			return commonpb.NewBigInt(result)
		},
		kb: data.NewKeyBuilder(),
	}
}

// NewMetadataAttribute creates a new Metadata attribute for account metadata.
func NewMetadataAttribute() *Attribute[*commonpb.MetadataValue] {
	return &Attribute[*commonpb.MetadataValue]{
		prefix:   data.AttributePrefixMetadata,
		newValue: func() *commonpb.MetadataValue { return &commonpb.MetadataValue{} },
		computeFn: func(base *commonpb.MetadataValue, diffs []*commonpb.MetadataValue) *commonpb.MetadataValue {
			if len(diffs) == 0 {
				return base
			}
			return diffs[len(diffs)-1]
		},
		kb: data.NewKeyBuilder(),
	}
}

// NewLedgerMetadataAttribute creates a new LedgerMetadata attribute for ledger metadata.
func NewLedgerMetadataAttribute() *Attribute[*commonpb.MetadataValue] {
	return &Attribute[*commonpb.MetadataValue]{
		prefix:   data.AttributePrefixLedgerMetadata,
		newValue: func() *commonpb.MetadataValue { return &commonpb.MetadataValue{} },
		computeFn: func(base *commonpb.MetadataValue, diffs []*commonpb.MetadataValue) *commonpb.MetadataValue {
			if len(diffs) == 0 {
				return base
			}
			return diffs[len(diffs)-1]
		},
		kb: data.NewKeyBuilder(),
	}
}

// NewRevertedAttribute creates a new Reverted attribute for tracking transaction reversion status.
func NewRevertedAttribute() *Attribute[*commonpb.RevertedValue] {
	return &Attribute[*commonpb.RevertedValue]{
		prefix:   data.AttributePrefixReverted,
		newValue: func() *commonpb.RevertedValue { return &commonpb.RevertedValue{} },
		computeFn: func(base *commonpb.RevertedValue, diffs []*commonpb.RevertedValue) *commonpb.RevertedValue {
			if len(diffs) == 0 {
				if base == nil {
					return &commonpb.RevertedValue{Reverted: false}
				}
				return base
			}
			return diffs[len(diffs)-1]
		},
		kb: data.NewKeyBuilder(),
	}
}

// NewIdempotencyKeysAttribute creates a new IdempotencyKeys attribute for storing idempotency key mappings.
func NewIdempotencyKeysAttribute() *Attribute[*commonpb.IdempotencyKeyValue] {
	return &Attribute[*commonpb.IdempotencyKeyValue]{
		prefix:   data.AttributePrefixIdempotencyKey,
		newValue: func() *commonpb.IdempotencyKeyValue { return &commonpb.IdempotencyKeyValue{} },
		computeFn: func(base *commonpb.IdempotencyKeyValue, diffs []*commonpb.IdempotencyKeyValue) *commonpb.IdempotencyKeyValue {
			if base != nil {
				return base
			}
			if len(diffs) > 0 {
				return diffs[0]
			}
			return nil
		},
		kb: data.NewKeyBuilder(),
	}
}

// Module returns the fx module for the attributes package.
func Module() fx.Option {
	return fx.Provide(New)
}
