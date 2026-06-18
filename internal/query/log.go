package query

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/coldstorage"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// ReadLastLog returns the full last log entry from the given reader. Returns nil if no logs exist.
func ReadLastLog(reader dal.PebbleReader) (*commonpb.Log, error) {
	log, err := dal.ReadLastEntry[*commonpb.Log](reader, dal.ZoneCold, dal.SubColdLog)
	if err != nil {
		return nil, fmt.Errorf("reading last log: %w", err)
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

	return log.GetSequence(), nil
}

// ReadLogBySequence retrieves a log by its sequence number from the given reader.
func ReadLogBySequence(ctx context.Context, reader dal.PebbleGetter, sequence uint64) (*commonpb.Log, error) {
	_, span := queryTracer.Start(ctx, "query.get_log",
		trace.WithAttributes(attribute.Int64("sequence", int64(sequence))))
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneCold, dal.SubColdLog).
		PutUint64(sequence)

	log, err := dal.ReadProto[*commonpb.Log](reader, kb.Build())
	if err != nil {
		return nil, fmt.Errorf("getting system log by sequence: %w", err)
	}

	return log, nil
}

// ledgerLogCursor iterates over pre-fetched global sequences and fetches full
// Log entries from Pebble on demand. It holds no long-lived resources.
type ledgerLogCursor struct {
	pebble dal.PebbleGetter
	seqs   []uint64
	pos    int
}

func (c *ledgerLogCursor) Next() (*commonpb.Log, error) {
	if c.pos >= len(c.seqs) {
		return nil, io.EOF
	}

	seq := c.seqs[c.pos]
	c.pos++

	log, err := ReadLogBySequence(context.Background(), c.pebble, seq)
	if err != nil {
		return nil, err
	}

	if log == nil {
		return nil, fmt.Errorf("log with sequence %d not found in Pebble", seq)
	}

	return log, nil
}

func (c *ledgerLogCursor) Close() error { return nil }

// ReadLedgerLogsCompiled returns a cursor over log entries using pre-compiled
// logID bytes from the Compile framework. It resolves logIDs → global sequences
// via the read index, then fetches the full Log from Pebble for each entry.
//
// Any structural inconsistency between the filter index (source of logIDs)
// and the per-ledger log index (lookup target) is surfaced as
// ErrIndexInconsistent rather than silently skipped — see #192. A
// truncated query result is worse than a clear error because the caller
// cannot tell it apart from a legitimate empty result.
func ReadLedgerLogsCompiled(
	pebbleReader dal.PebbleGetter,
	indexReader dal.PebbleGetter,
	ledgerName string,
	logIDs [][]byte,
) (cursor.Cursor[*commonpb.Log], error) {
	indexName := fmt.Sprintf("ledger-log[ledger=%s]", ledgerName)

	kb := dal.NewKeyBuilder()
	seqs := make([]uint64, 0, len(logIDs))

	for _, logIDBytes := range logIDs {
		if len(logIDBytes) != 8 {
			return nil, &domain.ErrIndexInconsistent{
				Index: indexName,
				Detail: fmt.Sprintf(
					"filter index produced a logID of unexpected length %d (want 8)",
					len(logIDBytes)),
			}
		}

		logID := binary.BigEndian.Uint64(logIDBytes)
		key := readstore.LedgerLogKey(kb, ledgerName, logID)

		v, closer, err := indexReader.Get(key)
		if err != nil {
			// Even pebble.ErrNotFound is suspect here: the filter index
			// produced this logID, so the per-ledger log index entry
			// should exist. A miss means the two are out of sync.
			return nil, &domain.ErrIndexInconsistent{
				Index: indexName,
				Detail: fmt.Sprintf(
					"reading per-ledger log index for logID=%d: %v",
					logID, err),
			}
		}

		if len(v) != 8 {
			_ = closer.Close()

			return nil, &domain.ErrIndexInconsistent{
				Index: indexName,
				Detail: fmt.Sprintf(
					"per-ledger log index value for logID=%d has unexpected length %d (want 8)",
					logID, len(v)),
			}
		}

		seqs = append(seqs, binary.BigEndian.Uint64(v))
		_ = closer.Close()
	}

	return &ledgerLogCursor{pebble: pebbleReader, seqs: seqs}, nil
}

// ReadLogsSinceRaw returns a raw Pebble iterator for logs after the given
// sequence. The caller receives raw key/value bytes without proto
// deserialization and is responsible for closing the iterator.
// The iterator is already positioned at the first valid entry (via First()).
func ReadLogsSinceRaw(_ context.Context, reader dal.PebbleReader, afterSequence uint64) (*pebble.Iterator, error) {
	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneCold, dal.SubColdLog)

	if afterSequence > 0 {
		kb.PutUint64(afterSequence + 1)
	}

	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutZonePrefix(dal.ZoneCold, dal.SubColdLog).
		PutBytes(dal.MaxUint64Bytes)
	upperBound := kb2.Build()

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating raw iterator for logs: %w", err)
	}

	return iter, nil
}

// ReadLogsSince returns a cursor over global log entries after the given sequence from the given reader.
// Pass afterSequence=0 to return all log entries.
func ReadLogsSince(ctx context.Context, reader dal.PebbleReader, afterSequence uint64, opts ...dal.ProtoCursorOption) (cursor.Cursor[*commonpb.Log], error) {
	_, span := queryTracer.Start(ctx, "query.list_logs")
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneCold, dal.SubColdLog)

	if afterSequence > 0 {
		kb.PutUint64(afterSequence + 1)
	}

	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutZonePrefix(dal.ZoneCold, dal.SubColdLog).
		PutBytes(dal.MaxUint64Bytes)
	upperBound := kb2.Build()

	iter, err := dal.NewBoundedIter(reader, lowerBound, upperBound)
	if err != nil {
		return nil, fmt.Errorf("creating iterator for logs: %w", err)
	}

	return dal.NewProtoCursor[*commonpb.Log](iter, opts...), nil
}

// ReadLogBySequenceWithCold tries hot storage first, then falls back to cold storage
// by finding the archived period that contains the given sequence.
func ReadLogBySequenceWithCold(
	ctx context.Context,
	hotReader dal.PebbleReader,
	coldReader *coldstorage.ColdReader,
	sequence uint64,
) (*commonpb.Log, error) {
	// Try hot storage first
	log, err := ReadLogBySequence(ctx, hotReader, sequence)
	if err != nil {
		return nil, err
	}

	if log != nil {
		return log, nil
	}

	// Hot miss — if no cold reader, return nil
	if coldReader == nil {
		return nil, nil
	}

	// Find the archived period containing this sequence
	periodID, err := findArchivedPeriodForSequence(ctx, hotReader, sequence)
	if err != nil {
		return nil, fmt.Errorf("finding archived period for sequence %d: %w", sequence, err)
	}

	if periodID == 0 {
		return nil, nil // not in any archived period
	}

	// Read from cold storage
	coldPebble, err := coldReader.GetReader(ctx, periodID)
	if err != nil {
		return nil, fmt.Errorf("getting cold reader for period %d: %w", periodID, err)
	}

	return ReadLogBySequence(ctx, coldPebble, sequence)
}

// findArchivedPeriodForSequence iterates periods to find an archived one containing the given sequence.
func findArchivedPeriodForSequence(ctx context.Context, reader dal.PebbleReader, sequence uint64) (uint64, error) {
	cursor, err := ReadPeriods(ctx, reader)
	if err != nil {
		return 0, err
	}

	defer func() { _ = cursor.Close() }()

	for {
		period, err := cursor.Next()
		if errors.Is(err, io.EOF) {
			break
		}

		if err != nil {
			return 0, err
		}

		if period.GetStatus() != commonpb.PeriodStatus_PERIOD_ARCHIVED {
			continue
		}

		if sequence >= period.GetStartSequence() && sequence <= period.GetCloseSequence() {
			return period.GetId(), nil
		}
	}

	return 0, nil
}
