package query

import (
	"context"
	"errors"
	"fmt"
	"io"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// mirrorPointKey builds a per-ledger mirror point key: [zone][sub][ledgerName padded 64B].
func mirrorPointKey(kb *dal.KeyBuilder, sub byte, ledgerName string) []byte {
	return kb.Reset().
		PutZonePrefix(dal.ZonePerLedger, sub).
		PutLedgerNameFixed(ledgerName).
		Build()
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

// ReadMirrorSyncProgress derives mirror sync progress for a ledger from the
// FSM-applied high-water mark (LedgerBoundaries.last_mirror_v2_log_id), the
// source head, and the recorded error. There is no separate persisted cursor:
// the boundary is the sole durable ingestion-position authority (EN-1513).
func ReadMirrorSyncProgress(
	ctx context.Context,
	reader dal.PebbleGetter,
	boundaries *attributes.Attribute[*raftcmdpb.LedgerBoundaries],
	ledgerName string,
) (*commonpb.MirrorSyncProgress, error) {
	_, span := queryTracer.Start(ctx, "query.read_mirror_sync_progress",
		trace.WithAttributes(attribute.String("ledger", ledgerName)))
	defer span.End()

	b, err := boundaries.Get(reader, []byte(ledgerName))
	if err != nil {
		return nil, fmt.Errorf("reading ledger boundaries: %w", err)
	}

	cursor := b.GetLastMirrorV2LogId()

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

		if info.GetMode() == commonpb.LedgerMode_LEDGER_MODE_MIRROR && info.GetDeletedAt() == nil {
			result = append(result, info)
		}
	}

	return result, nil
}
