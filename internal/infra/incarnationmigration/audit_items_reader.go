package incarnationmigration

import (
	"context"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// auditItemsReader follows the global audit-item range in audit order. Items
// returned by read are borrowed until the next call, which reuses the slice.
type auditItemsReader struct {
	iter     *pebble.Iterator
	started  bool
	previous uint64
	items    []*auditpb.AuditItem
}

func newAuditItemsReader(reader dal.PebbleReader) (*auditItemsReader, error) {
	lower := []byte{dal.ZoneHistory, dal.SubHistoryAuditItem}
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: dal.PrefixUpperBound(lower),
	})
	if err != nil {
		return nil, fmt.Errorf("opening audit item stream: %w", err)
	}

	return &auditItemsReader{iter: iter}, nil
}

func (r *auditItemsReader) close() error { return r.iter.Close() }

func (r *auditItemsReader) read(ctx context.Context, sequence uint64) ([]*auditpb.AuditItem, error) {
	if sequence <= r.previous {
		return nil, fmt.Errorf("audit item stream is not advancing: %d after %d", sequence, r.previous)
	}
	r.previous = sequence
	r.items = r.items[:0]
	if !r.started {
		r.iter.First()
		r.started = true
	}
	for r.iter.Valid() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := r.iter.Key()
		// A shorter malformed key sorts before all audit sequences. The normal
		// per-sequence range read excludes it too; the store checker reports it.
		if len(key) < 10 {
			r.iter.Next()

			continue
		}
		itemSequence := binary.BigEndian.Uint64(key[2:10])
		if itemSequence < sequence {
			r.iter.Next()

			continue
		}
		if itemSequence > sequence {
			break
		}
		value, err := r.iter.ValueAndErr()
		if err != nil {
			return nil, fmt.Errorf("reading audit item %d: %w", sequence, err)
		}
		item := &auditpb.AuditItem{}
		if err := proto.Unmarshal(value, item); err != nil {
			return nil, fmt.Errorf("decoding audit item %d: %w", sequence, err)
		}
		r.items = append(r.items, item)
		r.iter.Next()
	}
	if err := r.iter.Error(); err != nil {
		return nil, fmt.Errorf("reading audit item stream: %w", err)
	}

	return r.items, nil
}
