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
	Volume           *Attribute[*raftcmdpb.VolumePair]
	Metadata         *Attribute[*commonpb.MetadataValue]
	References       *Attribute[*commonpb.TransactionReferenceValue]
	Ledger           *Attribute[*commonpb.LedgerInfo]
	Boundary         *Attribute[*raftcmdpb.LedgerBoundaries]
	Transaction      *Attribute[*commonpb.TransactionState]
	SinkConfig       *Attribute[*commonpb.SinkConfig]
	NumscriptVersion *Attribute[*commonpb.NumscriptVersionValue]
	NumscriptContent *Attribute[*commonpb.NumscriptInfo]
	PreparedQuery    *Attribute[*commonpb.PreparedQuery]
	LedgerMetadata   *Attribute[*commonpb.MetadataValue]
}

// New creates a new Attributes instance with all attribute types initialized.
func New() *Attributes {
	return &Attributes{
		Volume:           NewVolumeAttribute(),
		Metadata:         NewMetadataAttribute(),
		References:       NewReferenceAttribute(),
		Ledger:           NewLedgerAttribute(),
		Boundary:         NewBoundaryAttribute(),
		Transaction:      NewTransactionAttribute(),
		SinkConfig:       NewSinkConfigAttribute(),
		NumscriptVersion: NewNumscriptVersionAttribute(),
		NumscriptContent: NewNumscriptContentAttribute(),
		PreparedQuery:    NewPreparedQueryAttribute(),
		LedgerMetadata:   NewLedgerMetadataAttribute(),
	}
}

// NewVolumeAttribute creates a new Volume attribute storing Input+Output pairs (last-write-wins).
func NewVolumeAttribute() *Attribute[*raftcmdpb.VolumePair] {
	return &Attribute[*raftcmdpb.VolumePair]{
		prefix:   dal.SubAttrVolume,
		newValue: func() *raftcmdpb.VolumePair { return &raftcmdpb.VolumePair{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewMetadataAttribute creates a new Metadata attribute for account metadata.
func NewMetadataAttribute() *Attribute[*commonpb.MetadataValue] {
	return &Attribute[*commonpb.MetadataValue]{
		prefix:   dal.SubAttrMetadata,
		newValue: func() *commonpb.MetadataValue { return &commonpb.MetadataValue{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewReferenceAttribute creates a new Reference attribute for storing transaction reference mappings.
func NewReferenceAttribute() *Attribute[*commonpb.TransactionReferenceValue] {
	return &Attribute[*commonpb.TransactionReferenceValue]{
		prefix:   dal.SubAttrReference,
		newValue: func() *commonpb.TransactionReferenceValue { return &commonpb.TransactionReferenceValue{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewLedgerAttribute creates a new Ledger attribute for storing ledger info.
func NewLedgerAttribute() *Attribute[*commonpb.LedgerInfo] {
	return &Attribute[*commonpb.LedgerInfo]{
		prefix:   dal.SubAttrLedger,
		newValue: func() *commonpb.LedgerInfo { return &commonpb.LedgerInfo{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewBoundaryAttribute creates a new Boundary attribute for storing ledger boundaries.
func NewBoundaryAttribute() *Attribute[*raftcmdpb.LedgerBoundaries] {
	return &Attribute[*raftcmdpb.LedgerBoundaries]{
		prefix:   dal.SubAttrBoundary,
		newValue: func() *raftcmdpb.LedgerBoundaries { return &raftcmdpb.LedgerBoundaries{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewTransactionAttribute creates a new Transaction attribute for storing transaction state.
func NewTransactionAttribute() *Attribute[*commonpb.TransactionState] {
	return &Attribute[*commonpb.TransactionState]{
		prefix:   dal.SubAttrTransaction,
		newValue: func() *commonpb.TransactionState { return &commonpb.TransactionState{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewSinkConfigAttribute creates a new SinkConfig attribute for storing event sink configurations.
func NewSinkConfigAttribute() *Attribute[*commonpb.SinkConfig] {
	return &Attribute[*commonpb.SinkConfig]{
		prefix:   dal.SubAttrSinkConfig,
		newValue: func() *commonpb.SinkConfig { return &commonpb.SinkConfig{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewNumscriptVersionAttribute creates a new NumscriptVersion attribute for storing latest version pointers.
func NewNumscriptVersionAttribute() *Attribute[*commonpb.NumscriptVersionValue] {
	return &Attribute[*commonpb.NumscriptVersionValue]{
		prefix:   dal.SubAttrNumscriptVersion,
		newValue: func() *commonpb.NumscriptVersionValue { return &commonpb.NumscriptVersionValue{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewNumscriptContentAttribute creates a new NumscriptContent attribute for storing full numscript info.
func NewNumscriptContentAttribute() *Attribute[*commonpb.NumscriptInfo] {
	return &Attribute[*commonpb.NumscriptInfo]{
		prefix:   dal.SubAttrNumscriptContent,
		newValue: func() *commonpb.NumscriptInfo { return &commonpb.NumscriptInfo{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewPreparedQueryAttribute creates a new PreparedQuery attribute for storing prepared queries.
func NewPreparedQueryAttribute() *Attribute[*commonpb.PreparedQuery] {
	return &Attribute[*commonpb.PreparedQuery]{
		prefix:   dal.SubAttrPreparedQuery,
		newValue: func() *commonpb.PreparedQuery { return &commonpb.PreparedQuery{} },
		keyBuf:   make([]byte, 128),
	}
}

// NewLedgerMetadataAttribute creates a new LedgerMetadata attribute for storing metadata on ledgers.
func NewLedgerMetadataAttribute() *Attribute[*commonpb.MetadataValue] {
	return &Attribute[*commonpb.MetadataValue]{
		prefix:   dal.SubAttrLedgerMetadata,
		newValue: func() *commonpb.MetadataValue { return &commonpb.MetadataValue{} },
		keyBuf:   make([]byte, 128),
	}
}

// Module returns the fx module for the attributes package.
func Module() fx.Option {
	return fx.Provide(New)
}
