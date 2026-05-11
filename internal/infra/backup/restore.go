package backup

import (
	"context"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ApplyExports downloads export segments from storage and writes their raw KV
// pairs into the given Pebble store. The entries are log (0x01) and audit (0x02)
// key-value pairs written directly — no deserialization is needed.
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

		batch := store.NewBatch()

		var count uint64

		for {
			key, value, err := kvReader.ReadEntry()
			if err != nil {
				_ = reader.Close()
				_ = batch.Cancel()

				return fmt.Errorf("reading entry from segment %s: %w", seg.Key, err)
			}

			if key == nil {
				break // EOF sentinel
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

				batch = store.NewBatch()
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
