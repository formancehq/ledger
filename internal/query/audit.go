package query

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ReadLastAuditSequence returns the last audit entry sequence from the given reader. Returns 0 if no entries exist.
func ReadLastAuditSequence(reader dal.PebbleReader) (uint64, error) {
	entry, err := ReadLastAuditEntry(reader)
	if err != nil {
		return 0, err
	}

	if entry == nil {
		return 0, nil
	}

	return entry.GetSequence(), nil
}

// ReadLastAuditEntry returns the last audit entry from the given reader, or nil if none exist.
func ReadLastAuditEntry(reader dal.PebbleReader) (*auditpb.AuditEntry, error) {
	entry, err := dal.ReadLastEntry[*auditpb.AuditEntry](reader, dal.ZoneCold, dal.SubColdAudit)
	if err != nil {
		return nil, fmt.Errorf("reading last audit entry: %w", err)
	}

	return entry, nil
}

// ReadAuditEntries returns a cursor over audit entries after the given sequence from the given reader.
// Use afterSequence=nil to return all entries, or a pointer to a sequence to filter.
func ReadAuditEntries(ctx context.Context, reader dal.PebbleReader, afterSequence *uint64) (cursor.Cursor[*auditpb.AuditEntry], error) {
	_, span := queryTracer.Start(ctx, "query.list_audit_entries")
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneCold, dal.SubColdAudit)

	if afterSequence != nil {
		kb.PutUint64(*afterSequence + 1)
	}

	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).
		PutBytes(dal.MaxUint64Bytes)
	upperBound := kb2.Build()

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for audit entries: %w", err)
	}

	return dal.NewProtoCursor[*auditpb.AuditEntry](iter), nil
}

// ReadAuditItems returns all audit items for the given audit sequence.
// Items are returned sorted by order_index (natural Pebble key order).
func ReadAuditItems(ctx context.Context, reader dal.PebbleReader, auditSequence uint64) ([]*auditpb.AuditItem, error) {
	_, span := queryTracer.Start(ctx, "query.read_audit_items",
		trace.WithAttributes(attribute.Int64("audit_sequence", int64(auditSequence))))
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).PutUint64(auditSequence)
	lowerBound := kb.Snapshot()
	kb.Reset()

	kb.PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).PutUint64(auditSequence + 1)
	upperBound := kb.Build()

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for audit items: %w", err)
	}

	defer func() { _ = iter.Close() }()

	var items []*auditpb.AuditItem

	for iter.First(); iter.Valid(); iter.Next() {
		value, valErr := iter.ValueAndErr()
		if valErr != nil {
			return nil, fmt.Errorf("reading audit item value: %w", valErr)
		}

		item := &auditpb.AuditItem{}
		if unmarshalErr := proto.Unmarshal(value, item); unmarshalErr != nil {
			return nil, fmt.Errorf("unmarshaling audit item: %w", unmarshalErr)
		}

		items = append(items, item)
	}

	return items, nil
}

// ReadAuditEntry returns a single audit entry by sequence number.
// Returns domain.ErrNotFound if the entry does not exist.
func ReadAuditEntry(ctx context.Context, reader dal.PebbleReader, sequence uint64) (*auditpb.AuditEntry, error) {
	_, span := queryTracer.Start(ctx, "query.get_audit_entry",
		trace.WithAttributes(attribute.Int64("sequence", int64(sequence))))
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(sequence)

	entry, err := dal.ReadProto[*auditpb.AuditEntry](reader, kb.Build())
	if err != nil {
		return nil, fmt.Errorf("reading audit entry %d: %w", sequence, err)
	}

	if entry == nil {
		return nil, domain.ErrNotFound
	}

	return entry, nil
}
