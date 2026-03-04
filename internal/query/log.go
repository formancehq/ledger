package query

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadLastLog returns the full last log entry from the given reader. Returns nil if no logs exist.
func ReadLastLog(reader dal.PebbleReader) (*commonpb.Log, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixLog)
	lowerBound := kb.Snapshot()
	kb.Reset()

	kb.PutByte(dal.KeyPrefixLog).
		PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	upperBound := kb.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator: %w", err)
	}
	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		return nil, nil
	}

	value, err := iter.ValueAndErr()
	if err != nil {
		return nil, fmt.Errorf("reading log value: %w", err)
	}

	log := &commonpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return nil, fmt.Errorf("unmarshaling log: %w", err)
	}

	return log, nil
}

// ReadLastSequence returns the last log sequence number from the given reader.
// Returns 0 if no logs exist. Reuses ReadLastLog to avoid duplicating the iterator logic.
func ReadLastSequence(reader dal.PebbleReader) (uint64, error) {
	log, err := ReadLastLog(reader)
	if err != nil {
		return 0, err
	}
	if log == nil {
		return 0, nil
	}
	return log.Sequence, nil
}

// ReadLogBySequence retrieves a log by its sequence number from the given reader.
func ReadLogBySequence(ctx context.Context, reader dal.PebbleReader, sequence uint64) (*commonpb.Log, error) {
	_, span := queryTracer.Start(ctx, "query.get_log",
		trace.WithAttributes(attribute.Int64("sequence", int64(sequence))))
	defer span.End()
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixLog).
		PutUInt64(sequence)

	value, closer, err := reader.Get(kb.Build())
	if err != nil {
		if err == pebble.ErrNotFound {
			return nil, nil
		}
		return nil, fmt.Errorf("getting system log by sequence: %w", err)
	}
	defer func() {
		_ = closer.Close()
	}()

	log := &commonpb.Log{}
	if err := proto.Unmarshal(value, log); err != nil {
		return nil, fmt.Errorf("unmarshaling system log from protobuf: %w", err)
	}

	return log, nil
}

// ReadLogsSince returns a cursor over global log entries after the given sequence from the given reader.
// Pass afterSequence=0 to return all log entries.
func ReadLogsSince(ctx context.Context, reader dal.PebbleReader, afterSequence uint64, opts ...dal.ProtoCursorOption) (dal.Cursor[*commonpb.Log], error) {
	_, span := queryTracer.Start(ctx, "query.list_logs")
	defer span.End()
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixLog)
	if afterSequence > 0 {
		kb.PutUInt64(afterSequence + 1)
	}
	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutByte(dal.KeyPrefixLog).
		PutBytes([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	upperBound := kb2.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating iterator for logs: %w", err)
	}

	return dal.NewProtoCursor[*commonpb.Log](iter, opts...), nil
}
