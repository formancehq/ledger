package query

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ReadMirrorCursor returns the last ingested v2 log ID for a mirror ledger.
// Returns 0 if no cursor has been persisted yet.
func ReadMirrorCursor(reader dal.PebbleReader, ledgerName string) (uint64, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixMirrorCursor).
		PutString(ledgerName)

	get, closer, err := reader.Get(kb.Build())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("reading mirror cursor: %w", err)
	}
	defer func() { _ = closer.Close() }()

	if len(get) < 8 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(get[:8]), nil
}

// ReadMirrorStatus returns the last sync error for a mirror ledger.
// Returns nil if no error is recorded.
func ReadMirrorStatus(reader dal.PebbleReader, ledgerName string) (*commonpb.MirrorSyncError, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixMirrorStatus).
		PutString(ledgerName)

	get, closer, err := reader.Get(kb.Build())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil
		}
		return nil, fmt.Errorf("reading mirror status: %w", err)
	}
	defer func() { _ = closer.Close() }()

	syncErr := &commonpb.MirrorSyncError{}
	if err := proto.Unmarshal(get, syncErr); err != nil {
		return nil, fmt.Errorf("unmarshaling mirror status: %w", err)
	}
	return syncErr, nil
}

// ReadMirrorSourceHead returns the latest known v2 source log count for a mirror ledger.
// Returns 0 if no source head has been persisted yet.
func ReadMirrorSourceHead(reader dal.PebbleReader, ledgerName string) (uint64, error) {
	kb := dal.NewKeyBuilder()
	kb.PutByte(dal.KeyPrefixMirrorSourceHead).
		PutString(ledgerName)

	get, closer, err := reader.Get(kb.Build())
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return 0, nil
		}
		return 0, fmt.Errorf("reading mirror source head: %w", err)
	}
	defer func() { _ = closer.Close() }()

	if len(get) < 8 {
		return 0, nil
	}

	return binary.BigEndian.Uint64(get[:8]), nil
}

// ReadMirrorSyncProgress reads the cursor, source head, and error from Pebble
// and computes the sync progress for a mirror ledger.
func ReadMirrorSyncProgress(ctx context.Context, reader dal.PebbleReader, ledgerName string) (*commonpb.MirrorSyncProgress, error) {
	_, span := queryTracer.Start(ctx, "query.read_mirror_sync_progress",
		trace.WithAttributes(attribute.String("ledger", ledgerName)))
	defer span.End()

	cursor, err := ReadMirrorCursor(reader, ledgerName)
	if err != nil {
		return nil, err
	}

	sourceHead, err := ReadMirrorSourceHead(reader, ledgerName)
	if err != nil {
		return nil, err
	}

	syncErr, err := ReadMirrorStatus(reader, ledgerName)
	if err != nil {
		return nil, err
	}

	state := commonpb.MirrorSyncState_MIRROR_SYNC_STATE_SYNCING
	if sourceHead > 0 && cursor >= sourceHead {
		state = commonpb.MirrorSyncState_MIRROR_SYNC_STATE_FOLLOWING
	}

	var remaining uint64
	if sourceHead > cursor {
		remaining = sourceHead - cursor
	}

	span.SetAttributes(
		attribute.Int64("mirror.cursor", int64(cursor)),
		attribute.Int64("mirror.source_head", int64(sourceHead)),
		attribute.Int64("mirror.remaining", int64(remaining)),
	)

	return &commonpb.MirrorSyncProgress{
		State:          state,
		Cursor:         cursor,
		SourceLogCount: sourceHead,
		RemainingLogs:  remaining,
		Error:          syncErr,
	}, nil
}

// ReadMirrorLedgers returns all ledgers in MIRROR mode.
func ReadMirrorLedgers(reader dal.PebbleReader) ([]*commonpb.LedgerInfo, error) {
	cursor, err := ReadLedgers(reader)
	if err != nil {
		return nil, fmt.Errorf("reading ledgers: %w", err)
	}
	defer func() { _ = cursor.Close() }()

	var result []*commonpb.LedgerInfo
	for {
		info, err := cursor.Next()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, fmt.Errorf("iterating ledgers: %w", err)
		}
		if info.Mode == commonpb.LedgerMode_LEDGER_MODE_MIRROR && info.DeletedAt == nil {
			result = append(result, info)
		}
	}
	return result, nil
}
