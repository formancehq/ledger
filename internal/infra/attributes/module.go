package attributes

import (
	"reflect"

	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
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
	Index            *Attribute[*commonpb.Index]
	Account          *Attribute[*commonpb.AccountState]
}

// New creates a new Attributes instance with all attribute types initialized.
func New() *Attributes {
	return &Attributes{
		Volume:           NewAttribute[*raftcmdpb.VolumePair](dal.SubAttrVolume),
		Metadata:         NewAttribute[*commonpb.MetadataValue](dal.SubAttrMetadata),
		References:       NewAttribute[*commonpb.TransactionReferenceValue](dal.SubAttrReference),
		Ledger:           NewAttribute[*commonpb.LedgerInfo](dal.SubAttrLedger),
		Boundary:         NewAttribute[*raftcmdpb.LedgerBoundaries](dal.SubAttrBoundary),
		Transaction:      NewAttribute[*commonpb.TransactionState](dal.SubAttrTransaction),
		SinkConfig:       NewAttribute[*commonpb.SinkConfig](dal.SubAttrSinkConfig),
		NumscriptVersion: NewAttribute[*commonpb.NumscriptVersionValue](dal.SubAttrNumscriptVersion),
		NumscriptContent: NewAttribute[*commonpb.NumscriptInfo](dal.SubAttrNumscriptContent),
		PreparedQuery:    NewAttribute[*commonpb.PreparedQuery](dal.SubAttrPreparedQuery),
		LedgerMetadata:   NewAttribute[*commonpb.MetadataValue](dal.SubAttrLedgerMetadata),
		Index:            NewAttribute[*commonpb.Index](dal.SubAttrIndex),
		Account:          NewAttribute[*commonpb.AccountState](dal.SubAttrAccount),
	}
}

// All returns every registered attribute as a type-erased slice, derived by
// reflection over the Attributes struct fields. Any field that implements
// anyAttribute (i.e. every *Attribute[V]) is included, so an attribute added to
// the struct and New() is automatically covered everywhere the full set is needed
// (e.g. the byte-for-byte preservation test that must exercise every attribute
// prefix). This is a one-time, off-the-hot-path enumeration over a handful of
// fields; reflection is already used in NewAttribute.
func (a *Attributes) All() []anyAttribute {
	v := reflect.ValueOf(a).Elem()

	out := make([]anyAttribute, 0, v.NumField())
	for _, field := range v.Fields() {
		// A field that does not implement anyAttribute (ok == false) is skipped;
		// every *Attribute[V] field from New() does, so all are collected.
		if attr, ok := field.Interface().(anyAttribute); ok {
			out = append(out, attr)
		}
	}

	return out
}

// NewAttribute creates a new Attribute for the given prefix byte.
// The proto.Message type V is instantiated via reflection.
func NewAttribute[V proto.Message](prefix byte) *Attribute[V] {
	var zero V
	elemType := reflect.TypeOf(zero).Elem()

	return &Attribute[V]{
		prefix:   prefix,
		newValue: func() V { return reflect.New(elemType).Interface().(V) },
		keyBuf:   make([]byte, 128),
	}
}

// Module returns the fx module for the attributes package.
func Module() fx.Option {
	return fx.Provide(New)
}
