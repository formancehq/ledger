package query

import (
	"context"
	"errors"
	"fmt"
	"slices"

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

// ReadAuditEntriesPage returns a bounded page of audit entries honoring an
// index-compiled filter, an opaque sequence cursor, reverse iteration and a
// page size — the audit analogue of the index-backed transaction/account list
// paths. It reads entries from reader (the audit zone in the main store) and,
// when the filter narrows to an explicit sequence set, resolves that set via
// the readstore audit index (already applied by the caller through
// CompileAuditFilter).
//
// Parameters:
//   - candidateSeqs / narrowed: the compiled filter's index result. When
//     narrowed is true, candidateSeqs is the ascending set of matching audit
//     sequences and only those are materialized. When false, every entry in
//     [loSeq, hiSeq] is a candidate.
//   - loSeq, hiSeq: inclusive audit-sequence bounds (from audit[seq] and from
//     the compiled filter). Defaults span the whole zone.
//   - afterSeq: opaque cursor; 0 means "from the head". Exclusive.
//   - reverse: false streams ascending by sequence (oldest first); true streams
//     descending (newest first).
//   - pageSize: maximum entries to return.
//
// Entries whose sequence is indexed but no longer present in the zone (chapter
// purge, per EN-1339) are skipped rather than erroring.
func ReadAuditEntriesPage(
	ctx context.Context,
	reader dal.PebbleReader,
	candidateSeqs []uint64,
	narrowed bool,
	loSeq, hiSeq uint64,
	afterSeq uint64,
	reverse bool,
	pageSize uint32,
) (cursor.Cursor[*auditpb.AuditEntry], error) {
	_, span := queryTracer.Start(ctx, "query.list_audit_entries_page",
		trace.WithAttributes(
			attribute.Bool("narrowed", narrowed),
			attribute.Bool("reverse", reverse),
			attribute.Int64("page_size", int64(pageSize)),
		))
	defer span.End()

	if narrowed {
		return readAuditPageFromSeqSet(reader, candidateSeqs, loSeq, hiSeq, afterSeq, reverse, pageSize)
	}

	return readAuditPageFromZone(reader, loSeq, hiSeq, afterSeq, reverse, pageSize)
}

// readAuditPageFromSeqSet materializes a page from an explicit, ascending set
// of candidate sequences. Cursor, bounds, reverse and page size are applied on
// the sequence slice before any entry is read, so only the page is fetched.
func readAuditPageFromSeqSet(
	reader dal.PebbleReader,
	seqs []uint64,
	loSeq, hiSeq, afterSeq uint64,
	reverse bool,
	pageSize uint32,
) (cursor.Cursor[*auditpb.AuditEntry], error) {
	// Restrict to the [loSeq, hiSeq] window carried by the compiled filter.
	filtered := seqs[:0:0]
	for _, s := range seqs {
		if s >= loSeq && s <= hiSeq {
			filtered = append(filtered, s)
		}
	}

	if reverse {
		slices.Reverse(filtered)
	}

	// Apply the exclusive cursor.
	if afterSeq != 0 {
		idx := 0
		for idx < len(filtered) {
			s := filtered[idx]
			if (!reverse && s <= afterSeq) || (reverse && s >= afterSeq) {
				idx++

				continue
			}

			break
		}
		filtered = filtered[idx:]
	}

	if pageSize > 0 && uint32(len(filtered)) > pageSize {
		filtered = filtered[:pageSize]
	}

	entries := make([]*auditpb.AuditEntry, 0, len(filtered))
	for _, s := range filtered {
		entry, err := ReadAuditEntry(context.Background(), reader, s)
		if errors.Is(err, domain.ErrNotFound) {
			// Indexed but purged from the zone — skip (EN-1339 tolerates
			// dangling index entries; drop+rebuild reclaims them).
			continue
		}
		if err != nil {
			return nil, fmt.Errorf("reading audit entry %d: %w", s, err)
		}

		entries = append(entries, entry)
	}

	return cursor.NewSliceCursor(entries), nil
}

// readAuditPageFromZone streams a page directly from the audit zone within the
// [loSeq, hiSeq] window, honoring the exclusive cursor and reverse iteration.
func readAuditPageFromZone(
	reader dal.PebbleReader,
	loSeq, hiSeq, afterSeq uint64,
	reverse bool,
	pageSize uint32,
) (cursor.Cursor[*auditpb.AuditEntry], error) {
	// Lower bound (inclusive): max(loSeq, afterSeq+1 in ascending).
	lo := loSeq
	hi := hiSeq

	if !reverse && afterSeq != 0 && afterSeq+1 > lo {
		lo = afterSeq + 1
	}
	if reverse && afterSeq != 0 && afterSeq >= 1 && afterSeq-1 < hi {
		hi = afterSeq - 1
	}
	if lo > hi {
		return cursor.NewSliceCursor([]*auditpb.AuditEntry{}), nil
	}

	kb := dal.NewKeyBuilder()
	lowerBound := kb.PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(lo).Build()

	kb2 := dal.NewKeyBuilder()
	var upperBound []byte
	if hi == ^uint64(0) {
		upperBound = kb2.PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutBytes(dal.MaxUint64Bytes).Build()
	} else {
		// Upper bound is exclusive, so hi+1 includes hi.
		upperBound = kb2.PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(hi + 1).Build()
	}

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for audit entries: %w", err)
	}

	entries := make([]*auditpb.AuditEntry, 0)
	valid := iter.Last
	if !reverse {
		valid = iter.First
	}

	for ok := valid(); ok; {
		value, valErr := iter.ValueAndErr()
		if valErr != nil {
			_ = iter.Close()

			return nil, fmt.Errorf("reading audit entry value: %w", valErr)
		}

		entry := &auditpb.AuditEntry{}
		if unmarshalErr := entry.UnmarshalVT(value); unmarshalErr != nil {
			_ = iter.Close()

			return nil, fmt.Errorf("unmarshaling audit entry: %w", unmarshalErr)
		}

		entries = append(entries, entry)
		if pageSize > 0 && uint32(len(entries)) >= pageSize {
			break
		}

		if reverse {
			ok = iter.Prev()
		} else {
			ok = iter.Next()
		}
	}

	if closeErr := iter.Close(); closeErr != nil {
		return nil, fmt.Errorf("closing audit entry iterator: %w", closeErr)
	}

	return cursor.NewSliceCursor(entries), nil
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
func ReadAuditEntry(ctx context.Context, reader dal.PebbleGetter, sequence uint64) (*auditpb.AuditEntry, error) {
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
