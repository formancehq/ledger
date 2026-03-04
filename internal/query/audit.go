package query

import (
	"context"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadLastAuditSequence returns the last audit entry sequence from the given reader. Returns 0 if no entries exist.
func ReadLastAuditSequence(reader dal.PebbleReader) (uint64, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixAudit)
	lowerBound := kb.Snapshot()
	kb.Reset()

	kb.PutByte(dal.KeyPrefixAudit).
		PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	upperBound := kb.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return 0, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		return 0, nil
	}

	value, err := iter.ValueAndErr()
	if err != nil {
		return 0, fmt.Errorf("reading audit value: %w", err)
	}

	entry := &auditpb.AuditEntry{}
	if err := proto.Unmarshal(value, entry); err != nil {
		return 0, fmt.Errorf("unmarshaling audit entry: %w", err)
	}

	return entry.Sequence, nil
}

// ReadAuditEntries returns a cursor over audit entries after the given sequence from the given reader.
// Use afterSequence=nil to return all entries, or a pointer to a sequence to filter.
func ReadAuditEntries(ctx context.Context, reader dal.PebbleReader, afterSequence *uint64) (dal.Cursor[*auditpb.AuditEntry], error) {
	_, span := queryTracer.Start(ctx, "query.list_audit_entries")
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixAudit)
	if afterSequence != nil {
		kb.PutUInt64(*afterSequence + 1)
	}
	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutByte(dal.KeyPrefixAudit).
		PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	upperBound := kb2.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for audit entries: %w", err)
	}

	return dal.NewProtoCursor[*auditpb.AuditEntry](iter), nil
}

// ReadAuditEntry returns a single audit entry by sequence number.
// Returns domain.ErrNotFound if the entry does not exist.
func ReadAuditEntry(ctx context.Context, reader dal.PebbleReader, sequence uint64) (*auditpb.AuditEntry, error) {
	_, span := queryTracer.Start(ctx, "query.get_audit_entry",
		trace.WithAttributes(attribute.Int64("sequence", int64(sequence))))
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixAudit).PutUInt64(sequence)
	key := kb.Build()

	value, closer, err := reader.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, domain.ErrNotFound
		}
		return nil, fmt.Errorf("reading audit entry %d: %w", sequence, err)
	}
	defer func() { _ = closer.Close() }()

	entry := &auditpb.AuditEntry{}
	if err := proto.Unmarshal(value, entry); err != nil {
		return nil, fmt.Errorf("unmarshaling audit entry %d: %w", sequence, err)
	}

	return entry, nil
}

// ReadAuditConfig loads the audit enabled flag from the given reader.
// Returns false if the config key does not exist (audit disabled by default).
func ReadAuditConfig(reader dal.PebbleReader) (bool, error) {
	value, closer, err := reader.Get([]byte{dal.KeyPrefixAuditConfig})
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, fmt.Errorf("loading audit config: %w", err)
	}
	defer func() { _ = closer.Close() }()

	if len(value) == 0 {
		return false, nil
	}
	return value[0] == 0x01, nil
}
