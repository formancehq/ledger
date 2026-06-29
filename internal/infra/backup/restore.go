package backup

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// ApplyExports downloads export segments from storage and writes their raw KV
// pairs into the given Pebble store. The segment types are log (0x01), audit
// (0x02), audit-item (0x03), and applied-proposal (0x04) — all restored as raw
// key-value pairs without deserialization.
func ApplyExports(
	ctx context.Context,
	logger logging.Logger,
	storage Storage,
	store *dal.Store,
	exports []ExportSegment,
) error {
	if len(exports) == 0 {
		return nil
	}

	for _, seg := range exports {
		logger.WithFields(map[string]any{
			"type":     seg.Type,
			"startSeq": seg.StartSeq,
			"endSeq":   seg.EndSeq,
			"key":      seg.Key,
		}).Infof("Applying export segment")

		reader, err := storage.GetFile(ctx, seg.Key)
		if err != nil {
			return fmt.Errorf("downloading segment %s: %w", seg.Key, err)
		}

		kvReader := NewKVStreamReader(reader)

		if err := kvReader.ReadHeader(); err != nil {
			_ = reader.Close()

			return fmt.Errorf("reading segment header %s: %w", seg.Key, err)
		}

		batch := store.OpenWriteSession()

		var count uint64

		for {
			key, value, err := kvReader.ReadEntry()
			if errors.Is(err, io.EOF) {
				break // footer sentinel: clean end of stream
			}

			if err != nil {
				_ = reader.Close()
				_ = batch.Cancel()

				return fmt.Errorf("reading entry from segment %s: %w", seg.Key, err)
			}

			if err := batch.Set(key, value, pebble.NoSync); err != nil {
				_ = reader.Close()
				_ = batch.Cancel()

				return fmt.Errorf("writing entry to store: %w", err)
			}

			count++

			// Commit in batches to avoid unbounded memory usage
			if count%10000 == 0 {
				if err := batch.Commit(); err != nil {
					_ = reader.Close()

					return fmt.Errorf("committing batch: %w", err)
				}

				batch = store.OpenWriteSession()
			}
		}

		_ = reader.Close()

		if err := batch.Commit(); err != nil {
			return fmt.Errorf("committing final batch for segment %s: %w", seg.Key, err)
		}

		logger.WithFields(map[string]any{
			"segment": seg.Key,
			"entries": count,
		}).Infof("Export segment applied")
	}

	return nil
}

// ApplyExportsAndRebuild applies the manifest's incremental export segments to
// the staged store and rebuilds derived state from them, leaving the staging
// directory fully restored. It is a no-op when the manifest carries no exports.
//
// Both the offline bootstrap (cmd/ledgerctl/store) and the gRPC restore path
// (internal/adapter/grpc) call this so the two cannot drift: skipping it loses
// every log/audit entry written after the last full checkpoint.
//
// The caller owns the *dal.Store lifecycle. Earlier versions opened and closed
// the store internally, but Pebble v2's in-process lock map could keep the
// staging directory marked as "locked" past Close (see formancehq/ledger#293),
// blocking the post-download validate/preview/finalize RPCs that need to open
// the same directory in the same process. Reusing one handle eliminates that
// re-open hazard.
func ApplyExportsAndRebuild(ctx context.Context, logger logging.Logger, storage Storage, store *dal.Store, manifest *Manifest) error {
	if manifest == nil || len(manifest.Exports) == 0 {
		return nil
	}

	if err := ApplyExports(ctx, logger, storage, store, manifest.Exports); err != nil {
		return fmt.Errorf("applying exports: %w", err)
	}

	// Derived state is rebuilt from the first log sequence not covered by the
	// checkpoint (0 when restoring from exports alone).
	var fromLogSeq uint64
	var fromAuditSeq uint64
	if manifest.Checkpoint != nil {
		fromLogSeq = manifest.Checkpoint.LastLogSequence
		fromAuditSeq = manifest.Checkpoint.LastAuditSequence
	}

	if err := RebuildDelta(ctx, logger, store, fromLogSeq, fromAuditSeq); err != nil {
		return fmt.Errorf("rebuilding derived state: %w", err)
	}

	return nil
}
