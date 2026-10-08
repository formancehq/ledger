package attributes

import (
	"reflect"

	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	internalstatepb "github.com/formancehq/ledger/v3/internal/proto/internalstatepb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Attributes holds all attribute types used in the ledger.
// Each instance has its own pre-allocated key buffer for thread-safe concurrent access.
type Attributes struct {
	Volume           *Attribute[*raftcmdpb.VolumePair]
	Metadata         *Attribute[*ledgerpb.MetadataValue]
	References       *Attribute[*internalstatepb.TransactionReferenceValue]
	Ledger           *Attribute[*ledgerpb.LedgerInfo]
	Boundary         *Attribute[*raftcmdpb.LedgerBoundaries]
	Transaction      *Attribute[*internalstatepb.TransactionState]
	SinkConfig       *Attribute[*ledgerpb.SinkConfig]
	NumscriptVersion *Attribute[*internalstatepb.NumscriptVersionValue]
	NumscriptContent *Attribute[*ledgerpb.NumscriptInfo]
	PreparedQuery    *Attribute[*ledgerpb.PreparedQuery]
	LedgerMetadata   *Attribute[*ledgerpb.MetadataValue]
	Index            *Attribute[*ledgerpb.Index]
}

// New creates a new Attributes instance with all attribute types initialized.
func New() *Attributes {
	return &Attributes{
		Volume:           NewAttribute[*raftcmdpb.VolumePair](dal.SubAttrVolume),
		Metadata:         NewAttribute[*ledgerpb.MetadataValue](dal.SubAttrMetadata),
		References:       NewAttribute[*internalstatepb.TransactionReferenceValue](dal.SubAttrReference),
		Ledger:           NewAttribute[*ledgerpb.LedgerInfo](dal.SubAttrLedger),
		Boundary:         NewAttribute[*raftcmdpb.LedgerBoundaries](dal.SubAttrBoundary),
		Transaction:      NewAttribute[*internalstatepb.TransactionState](dal.SubAttrTransaction),
		SinkConfig:       NewAttribute[*ledgerpb.SinkConfig](dal.SubAttrSinkConfig),
		NumscriptVersion: NewAttribute[*internalstatepb.NumscriptVersionValue](dal.SubAttrNumscriptVersion),
		NumscriptContent: NewAttribute[*ledgerpb.NumscriptInfo](dal.SubAttrNumscriptContent),
		PreparedQuery:    NewAttribute[*ledgerpb.PreparedQuery](dal.SubAttrPreparedQuery),
		LedgerMetadata:   NewAttribute[*ledgerpb.MetadataValue](dal.SubAttrLedgerMetadata),
		Index:            NewAttribute[*ledgerpb.Index](dal.SubAttrIndex),
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
		if attr, ok := reflect.TypeAssert[anyAttribute](field); ok {
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
