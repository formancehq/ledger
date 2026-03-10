package indexbuilder

import (
	"context"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
)

// processBackfillPostings is the fast path for backfilling posting-related
// indexes (ADDRESS, SOURCE_ADDRESS, DEST_ADDRESS). It reads raw Pebble values
// and uses a protowire parser that only extracts the fields needed for posting
// indexation (~30% of the payload), skipping metadata, balances, volumes,
// timestamps, signatures and hash.
//
// This reduces allocations from ~32/op (UnmarshalVT + resetLogForReuse) to ~5/op
// and avoids parsing ~70% of each log entry's bytes.
func (b *Builder) processBackfillPostings(stop <-chan struct{}, task *backfillTask, deadline time.Time) error {
	iter, err := query.ReadLogsSinceRaw(context.Background(), b.pebbleStore, task.cursor)
	if err != nil {
		return err
	}

	defer func() { _ = iter.Close() }()

	// Determine which address indexes are active.
	builtin, ok := task.index.transaction.GetKind().(*commonpb.TransactionIndex_Builtin)
	if !ok {
		return nil
	}

	indexAny := builtin.Builtin == commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS
	indexSrc := builtin.Builtin == commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS
	indexDst := builtin.Builtin == commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS

	var parsed parsedLog

	// Position the iterator at the first entry.
	iterValid := iter.First()

	for time.Now().Before(deadline) {
		select {
		case <-stop:
			return nil
		default:
		}

		var (
			batchCount int
			lastSeq    uint64
			eof        bool
		)

		if err := b.readStore.Update(func(tx *bolt.Tx) error {
			b.wb.Init(tx)

			for batchCount < backfillBatchSize {
				if !iterValid {
					eof = true

					break
				}

				value, verr := iter.ValueAndErr()
				if verr != nil {
					return verr
				}

				if err := parsePostingsFromLog(value, &parsed); err != nil {
					return err
				}

				lastSeq = parsed.Sequence
				batchCount++

				// Advance the iterator now (value was already consumed).
				iterValid = iter.Next()

				// Skip non-transaction logs (config mutations, metadata-only, etc.)
				if parsed.LogType == 0 {
					continue
				}

				kb := b.kb
				wb := b.wb

				for i := range parsed.Postings {
					p := &parsed.Postings[i]
					if indexAny {
						wb.WriteAccountTxMapping(kb, parsed.Ledger, p.Source, parsed.TxID)
						wb.WriteAccountTxMapping(kb, parsed.Ledger, p.Destination, parsed.TxID)
					}

					if indexSrc {
						wb.WriteSourceAccountTxMapping(kb, parsed.Ledger, p.Source, parsed.TxID)
					}

					if indexDst {
						wb.WriteDestAccountTxMapping(kb, parsed.Ledger, p.Destination, parsed.TxID)
					}
				}
			}

			// Persist backfill cursor.
			if batchCount > 0 {
				if err := b.wb.Flush(); err != nil {
					return err
				}

				return b.readStore.WriteBackfillProgress(tx, task.bbKey, lastSeq)
			}

			return nil
		}); err != nil {
			return err
		}

		if batchCount == 0 {
			break
		}

		task.cursor = lastSeq

		if eof {
			break
		}
	}

	return nil
}

// isPostingIndex returns true if the index is a transaction builtin index
// related to postings (ADDRESS, SOURCE_ADDRESS, DEST_ADDRESS).
func isPostingIndex(id indexID) bool {
	if id.transaction == nil {
		return false
	}

	builtin, ok := id.transaction.GetKind().(*commonpb.TransactionIndex_Builtin)
	if !ok {
		return false
	}

	switch builtin.Builtin {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS:
		return true
	}

	return false
}
