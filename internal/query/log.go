package query

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	bolt "go.etcd.io/bbolt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

// ReadLastLog returns the full last log entry from the given reader. Returns nil if no logs exist.
func ReadLastLog(reader dal.PebbleReader) (*commonpb.Log, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixLog)
	lowerBound := kb.Snapshot()
	kb.Reset()

	kb.PutByte(dal.KeyPrefixLog).
		PutBytes(dal.MaxUint64Bytes)
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

	return log.GetSequence(), nil
}

// ReadLogBySequence retrieves a log by its sequence number from the given reader.
func ReadLogBySequence(ctx context.Context, reader dal.PebbleReader, sequence uint64) (*commonpb.Log, error) {
	_, span := queryTracer.Start(ctx, "query.get_log",
		trace.WithAttributes(attribute.Int64("sequence", int64(sequence))))
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixLog).
		PutUint64(sequence)

	value, closer, err := reader.Get(kb.Build())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
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

// ledgerLogCursor iterates over pre-fetched global sequences and fetches full
// Log entries from Pebble on demand. It holds no long-lived resources.
type ledgerLogCursor struct {
	pebble dal.PebbleReader
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
// via BucketLedgerLogs, then fetches the full Log from Pebble for each entry.
func ReadLedgerLogsCompiled(
	pebbleReader dal.PebbleReader,
	tx *bolt.Tx,
	ledger string,
	logIDs [][]byte,
) (dal.Cursor[*commonpb.Log], error) {
	kb := dal.NewKeyBuilder()
	bucket := tx.Bucket(readstore.BucketLedgerLogs)

	if bucket == nil {
		return &ledgerLogCursor{pebble: pebbleReader}, nil
	}

	seqs := make([]uint64, 0, len(logIDs))

	for _, logIDBytes := range logIDs {
		if len(logIDBytes) != 8 {
			continue
		}

		logID := binary.BigEndian.Uint64(logIDBytes)
		key := readstore.LedgerLogKey(kb, ledger, logID)

		v := bucket.Get(key)
		if v == nil || len(v) != 8 {
			continue
		}

		seqs = append(seqs, binary.BigEndian.Uint64(v))
	}

	return &ledgerLogCursor{pebble: pebbleReader, seqs: seqs}, nil
}

// ReadLedgerLogsSince returns a cursor over log entries for a specific ledger,
// ordered by ledger-local log ID (ascending). Pass afterLogID=0 to start from
// the beginning. pageSize=0 means no limit.
//
// It reads the per-ledger index from bbolt (BucketLedgerLogs) to get global
// sequences, then fetches the full Log from Pebble for each entry.
func ReadLedgerLogsSince(
	_ context.Context,
	pebbleReader dal.PebbleReader,
	readStore *readstore.Store,
	ledger string,
	afterLogID uint64,
	pageSize uint32,
) (dal.Cursor[*commonpb.Log], error) {
	kb := dal.NewKeyBuilder()
	prefix := readstore.LedgerLogPrefix(kb, ledger)

	var startKey []byte
	if afterLogID > 0 {
		startKey = readstore.LedgerLogKey(kb, ledger, afterLogID+1)
	} else {
		startKey = prefix
	}

	var seqs []uint64

	err := readStore.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(readstore.BucketLedgerLogs)
		if b == nil {
			return nil
		}

		c := b.Cursor()
		for k, v := c.Seek(startKey); k != nil && bytes.HasPrefix(k, prefix); k, v = c.Next() {
			if len(v) != 8 {
				continue
			}

			seqs = append(seqs, binary.BigEndian.Uint64(v))
			if pageSize > 0 && uint32(len(seqs)) >= pageSize {
				break
			}
		}

		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("reading ledger log index: %w", err)
	}

	return &ledgerLogCursor{pebble: pebbleReader, seqs: seqs}, nil
}

// ReadLogsSinceRaw returns a raw Pebble iterator for logs after the given
// sequence. The caller receives raw key/value bytes without proto
// deserialization and is responsible for closing the iterator.
// The iterator is already positioned at the first valid entry (via First()).
func ReadLogsSinceRaw(_ context.Context, reader dal.PebbleReader, afterSequence uint64) (*pebble.Iterator, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixLog)

	if afterSequence > 0 {
		kb.PutUint64(afterSequence + 1)
	}

	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutByte(dal.KeyPrefixLog).
		PutBytes(dal.MaxUint64Bytes)
	upperBound := kb2.Build()

	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upperBound,
	})
	if err != nil {
		return nil, fmt.Errorf("creating raw iterator for logs: %w", err)
	}

	return iter, nil
}

// ReadLogsSince returns a cursor over global log entries after the given sequence from the given reader.
// Pass afterSequence=0 to return all log entries.
func ReadLogsSince(ctx context.Context, reader dal.PebbleReader, afterSequence uint64, opts ...dal.ProtoCursorOption) (dal.Cursor[*commonpb.Log], error) {
	_, span := queryTracer.Start(ctx, "query.list_logs")
	defer span.End()

	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixLog)

	if afterSequence > 0 {
		kb.PutUint64(afterSequence + 1)
	}

	lowerBound := kb.Build()

	kb2 := dal.NewKeyBuilder()
	kb2.PutByte(dal.KeyPrefixLog).
		PutBytes(dal.MaxUint64Bytes)
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
