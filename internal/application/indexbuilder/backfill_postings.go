package indexbuilder

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// processBackfillPostings is the fast path for backfilling posting-related
// indexes (ADDRESS, SOURCE_ADDRESS, DEST_ADDRESS). It reads raw Pebble values
// and uses a protowire parser that only extracts the fields needed for posting
// indexation (~30% of the payload), skipping metadata, balances, volumes,
// timestamps, signatures and hash.
//
// This reduces allocations from ~32/op (UnmarshalVT + resetLogForReuse) to ~5/op
// and avoids parsing ~70% of each log entry's bytes.
func (b *Builder) processBackfillPostings(ctx context.Context, stop <-chan struct{}, task *backfillTask, deadline time.Time) error {
	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		return fmt.Errorf("creating read handle for postings backfill: %w", err)
	}

	defer func() { _ = handle.Close() }()

	iter, err := query.ReadLogsSinceRaw(ctx, handle, task.cursor)
	if err != nil {
		return err
	}

	defer func() { _ = iter.Close() }()

	proposals, err := newAppliedProposalSync(ctx, handle, task.appliedProposalSeq)
	if err != nil {
		return fmt.Errorf("creating applied proposal sync for postings backfill: %w", err)
	}

	defer func() { _ = proposals.close() }()

	// Determine which posting-derived index this task drives. The shared
	// posting walk gates each output on a flag (address mappings) or on the
	// cfg (account-by-asset hook), so exactly one of the two backfill modes
	// is active per task.
	var (
		indexAny bool
		indexSrc bool
		indexDst bool
		// cfg controls the account-by-asset hook in indexPostingAddressMappings.
		// An empty config leaves ACCT_BUILTIN_INDEX_ASSET unregistered so the
		// hook is a no-op; only the account-asset task registers it.
		cfg = newLedgerIndexConfig()
	)

	switch k := task.index.GetKind().(type) {
	case *commonpb.IndexID_TxBuiltin:
		// A tx-address builtin task: turn on the matching address-mapping
		// flag and leave the account-by-asset hook off (cfg stays empty).
		indexAny = k.TxBuiltin == commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS
		indexSrc = k.TxBuiltin == commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS
		indexDst = k.TxBuiltin == commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS
	case *commonpb.IndexID_AccountBuiltin:
		if k.AccountBuiltin != commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET {
			// Unreachable by design: isPostingIndex only routes the account
			// has-asset builtin here. A miss means a misrouted task, which
			// would otherwise spin as a no-op forever with the index stuck
			// BUILDING — fail loud per invariant #7.
			return fmt.Errorf("invariant: processBackfillPostings reached with non-asset account builtin index %v", task.index.GetKind())
		}
		// Account has-asset task: register only the account-asset index so
		// the shared walk writes account-by-asset entries (all address-
		// mapping flags stay off).
		cfg.byCanonical[indexes.Canonical(task.index)] = &commonpb.Index{Id: task.index}
	default:
		// Unreachable by design: see above — isPostingIndex gates this
		// function to tx-address builtins and the account has-asset index.
		return fmt.Errorf("invariant: processBackfillPostings reached with non-posting index %v", task.index.GetKind())
	}

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

		batch := b.readStore.NewBatch()
		b.initBatch(batch)

		for batchCount < backfillBatchSize {
			if !iterValid {
				eof = true

				break
			}

			value, verr := iter.ValueAndErr()
			if verr != nil {
				_ = batch.Cancel()

				return verr
			}

			if err := parsePostingsFromLog(value, &parsed); err != nil {
				_ = batch.Cancel()

				return err
			}

			lastSeq = parsed.Sequence
			batchCount++

			// Advance the iterator now (value was already consumed).
			iterValid = iter.Next()

			// DeleteLedger: only the ledger under construction matters. The
			// backfill replays the GLOBAL log for a single-ledger task, so a
			// historical delete for any *other* ledger must NOT be acted on:
			// DeleteLedgerIndexes is a full wipe of every ledger-scoped prefix
			// (version state, backfill state, and all index keyspaces), so
			// firing it for an unrelated ledger would clobber that ledger's
			// live-maintained READY indexes. For task.ledger itself, wipe the
			// stale pre-recreate rows exactly as the live processLogs path does
			// — a delete + same-name recreate would otherwise leave stale
			// account-by-asset (and address-mapping) rows from the deleted
			// generation. markLedgerDeletedInBatch invalidates the in-batch
			// dedup state so the recreate's writes — queued after the range
			// delete and ordered after it at commit — are not suppressed.
			if parsed.DeletedLedger != "" {
				if parsed.DeletedLedger == task.ledger {
					if err := readstore.DeleteLedgerIndexes(b.wb.Batch(), parsed.DeletedLedger); err != nil {
						_ = batch.Cancel()

						return err
					}

					b.markLedgerDeletedInBatch(parsed.DeletedLedger)
				}

				continue
			}

			// This task only builds task.ledger's index, and the writes below
			// are keyed by parsed.Ledger — skip logs belonging to other ledgers.
			if parsed.Ledger != task.ledger {
				continue
			}

			// Skip non-transaction logs (config mutations, metadata-only, etc.)
			if parsed.LogType == 0 {
				continue
			}

			kb := b.kb
			excludedVolumes := proposals.excludedForLog(parsed.Sequence, parsed.Ledger, &parsed)

			for i := range parsed.Postings {
				p := &parsed.Postings[i]
				if err := b.indexPostingAddressMappings(
					kb, cfg, parsed.Ledger, parsed.TxID, p.Source, p.Destination, p.Asset,
					indexAny, indexSrc, indexDst, excludedVolumes,
				); err != nil {
					_ = batch.Cancel()

					return err
				}
			}
		}

		// AppliedProposal cursor errors set during excludedForLog must be
		// surfaced BEFORE the batch is flushed: any call in this batch
		// could have stashed an iterErr (coverage mismatch or corrupt
		// proto) and returned an empty exclusion set, in which case the
		// posting mappings already written into b.wb are incomplete.
		// Committing them would persist account->tx mappings for volumes
		// that should have been excluded.
		if err := proposals.err(); err != nil {
			_ = batch.Cancel()

			return fmt.Errorf("applied proposal cursor failed: %w", err)
		}

		// Persist backfill cursor and flush.
		if batchCount > 0 {
			if err := b.readStore.WriteBackfillProgress(batch, task.bbKey, lastSeq); err != nil {
				_ = batch.Cancel()

				return err
			}

			if err := b.wb.Flush(); err != nil {
				return err
			}
		} else {
			_ = batch.Cancel()
		}

		if batchCount == 0 {
			break
		}

		task.cursor = lastSeq
		proposals.advanceBefore(lastSeq + 1)
		if err := proposals.err(); err != nil {
			return fmt.Errorf("applied proposal cursor failed: %w", err)
		}
		task.appliedProposalSeq = proposals.resumeSequence()

		if eof {
			break
		}
	}

	return nil
}

// isPostingIndex returns true if the index is replayed through the shared
// posting walk during backfill: the transaction builtin address indexes
// (ADDRESS, SOURCE_ADDRESS, DEST_ADDRESS) and the account has-asset index.
func isPostingIndex(id *commonpb.IndexID) bool {
	switch k := id.GetKind().(type) {
	case *commonpb.IndexID_TxBuiltin:
		switch k.TxBuiltin {
		case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS,
			commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS,
			commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS:
			return true
		}
	case *commonpb.IndexID_AccountBuiltin:
		return k.AccountBuiltin == commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET
	}

	return false
}
