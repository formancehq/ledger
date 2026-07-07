package query

import (
	"context"
	"errors"
	"fmt"
	"io"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// mirrorPointKey builds a per-ledger mirror point key: [zone][sub][ledgerName padded 64B].
func mirrorPointKey(kb *dal.KeyBuilder, sub byte, ledgerName string) []byte {
	return kb.Reset().
		PutZonePrefix(dal.ZonePerLedger, sub).
		PutLedgerNameFixed(ledgerName).
		Build()
}

// ReadMirrorCursor returns the last ingested v2 log ID for a mirror ledger.
// Returns 0 if no cursor has been persisted yet.
func ReadMirrorCursor(reader dal.PebbleGetter, ledgerName string) (uint64, error) {
	kb := dal.NewKeyBuilder()
	key := mirrorPointKey(kb, dal.SubPLMirrorCursor, ledgerName)

	v, err := dal.ReadUint64(reader, key, 0)
	if err != nil {
		return 0, fmt.Errorf("reading mirror cursor: %w", err)
	}

	return v, nil
}

// ReadMirrorStatus returns the last sync error for a mirror ledger.
// Returns nil if no error is recorded.
func ReadMirrorStatus(reader dal.PebbleGetter, ledgerName string) (*commonpb.MirrorSyncError, error) {
	kb := dal.NewKeyBuilder()
	key := mirrorPointKey(kb, dal.SubPLMirrorStatus, ledgerName)

	syncErr, err := dal.ReadProto[*commonpb.MirrorSyncError](reader, key)
	if err != nil {
		return nil, fmt.Errorf("reading mirror status: %w", err)
	}

	return syncErr, nil
}

// ReadMirrorSourceHead returns the latest known v2 source log count for a mirror ledger.
// Returns 0 if no source head has been persisted yet.
func ReadMirrorSourceHead(reader dal.PebbleGetter, ledgerName string) (uint64, error) {
	kb := dal.NewKeyBuilder()
	key := mirrorPointKey(kb, dal.SubPLMirrorSourceHead, ledgerName)

	v, err := dal.ReadUint64(reader, key, 0)
	if err != nil {
		return 0, fmt.Errorf("reading mirror source head: %w", err)
	}

	return v, nil
}

// ReadMirrorSyncProgress reads the cursor, source head, and error from Pebble
// and computes the sync progress for a mirror ledger.
func ReadMirrorSyncProgress(ctx context.Context, reader dal.PebbleGetter, ledgerName string) (*commonpb.MirrorSyncProgress, error) {
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
func ReadMirrorLedgers(ctx context.Context, reader dal.PebbleReader) ([]*commonpb.LedgerInfo, error) {
	cursor, err := ReadLedgers(ctx, reader)
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

		if info.GetMode() == commonpb.LedgerMode_LEDGER_MODE_MIRROR && info.GetDeletedAt() == 0 {
			result = append(result, info)
		}
	}

	return result, nil
}
