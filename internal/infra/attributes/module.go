package attributes

import (
	"go.uber.org/fx"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// Attributes holds all attribute types used in the ledger.
// Each instance has its own pre-allocated key buffer for thread-safe concurrent access.
type Attributes struct {
	Volume          *Attribute[*raftcmdpb.VolumePair]
	Metadata        *Attribute[*commonpb.MetadataValue]
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
		IdempotencyKeys: NewIdempotencyKeysAttribute(),
		References:      NewReferenceAttribute(),
		Ledger:          NewLedgerAttribute(),
		Boundary:        NewBoundaryAttribute(),
	}
}

// NewVolumeAttribute creates a new Volume attribute storing Input+Output pairs (last-write-wins).
func NewVolumeAttribute() *Attribute[*raftcmdpb.VolumePair] {
	return &Attribute[*raftcmdpb.VolumePair]{
		prefix:   dal.AttributePrefixVolume,
		newValue: func() *raftcmdpb.VolumePair { return &raftcmdpb.VolumePair{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewMetadataAttribute creates a new Metadata attribute for account metadata.
func NewMetadataAttribute() *Attribute[*commonpb.MetadataValue] {
	return &Attribute[*commonpb.MetadataValue]{
		prefix:   dal.AttributePrefixMetadata,
		newValue: func() *commonpb.MetadataValue { return &commonpb.MetadataValue{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewIdempotencyKeysAttribute creates a new IdempotencyKeys attribute for storing idempotency key mappings.
func NewIdempotencyKeysAttribute() *Attribute[*commonpb.IdempotencyKeyValue] {
	return &Attribute[*commonpb.IdempotencyKeyValue]{
		prefix:   dal.AttributePrefixIdempotency,
		newValue: func() *commonpb.IdempotencyKeyValue { return &commonpb.IdempotencyKeyValue{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewReferenceAttribute creates a new Reference attribute for storing transaction reference mappings.
func NewReferenceAttribute() *Attribute[*commonpb.TransactionReferenceValue] {
	return &Attribute[*commonpb.TransactionReferenceValue]{
		prefix:   dal.AttributePrefixReference,
		newValue: func() *commonpb.TransactionReferenceValue { return &commonpb.TransactionReferenceValue{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewLedgerAttribute creates a new Ledger attribute for storing ledger info.
func NewLedgerAttribute() *Attribute[*commonpb.LedgerInfo] {
	return &Attribute[*commonpb.LedgerInfo]{
		prefix:   dal.AttributePrefixLedger,
		newValue: func() *commonpb.LedgerInfo { return &commonpb.LedgerInfo{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewBoundaryAttribute creates a new Boundary attribute for storing ledger boundaries.
func NewBoundaryAttribute() *Attribute[*raftcmdpb.LedgerBoundaries] {
	return &Attribute[*raftcmdpb.LedgerBoundaries]{
		prefix:   dal.AttributePrefixBoundary,
		newValue: func() *raftcmdpb.LedgerBoundaries { return &raftcmdpb.LedgerBoundaries{} },
		keyBuf:   make([]byte, 128),
	}
}

// Module returns the fx module for the attributes package.
func Module() fx.Option {
	return fx.Provide(New)
}
