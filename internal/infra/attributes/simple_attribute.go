package attributes

import (
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// SimpleAttribute is an attribute type for simple key-value storage.
// It exposes Set() for writing values. Used for Metadata, IdempotencyKeys,
// References, Ledger, and Boundary attributes.
type SimpleAttribute[V proto.Message] struct {
	core[V]
}

// Set stores a value for the given canonical key at the specified raft index.
// The canonical key is used directly as the Pebble key for better data locality.
// Note: Uses the instance's keyBuf — ensure each Raft node has its own instance.
func (a *SimpleAttribute[V]) Set(batch *dal.Batch, index uint64, canonicalKey []byte, value V) error {
	return a.setBase(batch, index, canonicalKey, value)
}
