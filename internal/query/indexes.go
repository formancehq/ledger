package query

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// pebbleIndexReader resolves Index entries against a Pebble snapshot of the
// main store's attribute zone (SubAttrIndex). It is the read-side counterpart
// to the WriteSet-backed lookup used by the FSM hot path: both implement
// indexes.Lookup, so callers of query.Compile can pass either.
type pebbleIndexReader struct {
	attr   *attributes.Attribute[*commonpb.Index]
	reader dal.PebbleGetter
}

// NewPebbleIndexReader wires a read-side indexes.Lookup.
//
// A nil attribute or reader returns a Lookup that reports
// (nil, domain.ErrNotFound) — useful for tests and ad-hoc Compile callers
// that have no index registry to consult.
func NewPebbleIndexReader(attr *attributes.Attribute[*commonpb.Index], reader dal.PebbleGetter) indexes.Lookup {
	return &pebbleIndexReader{attr: attr, reader: reader}
}

func (r *pebbleIndexReader) GetIndex(key domain.IndexKey) (commonpb.IndexReader, error) {
	if r == nil || r.attr == nil || r.reader == nil {
		return nil, domain.ErrNotFound
	}

	idx, err := r.attr.Get(r.reader, key.Bytes())
	if err != nil {
		return nil, err
	}

	if idx == nil {
		return nil, domain.ErrNotFound
	}

	return idx.AsReader(), nil
}
