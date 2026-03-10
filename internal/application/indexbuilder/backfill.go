package indexbuilder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/formancehq/ledger-v3-poc/internal/storage/readstore"
)

// backfillTask tracks the progress of backfilling a single index.
type backfillTask struct {
	ledger string
	index  indexID
	cursor uint64 // current position (persisted in bbolt)
	bbKey  []byte // precomputed bbolt key for progress persistence

	// Progress logging state.
	lastProgressLog time.Time // last time a progress log was emitted
	lastProgressSeq uint64    // cursor value at last progress log
}

// backfillIndexName returns a human-readable name for a backfill index ID.
func backfillIndexName(id indexID) string {
	if id.transaction != nil {
		switch txIdx := id.transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			return "tx:" + txIdx.Builtin.String()
		case *commonpb.TransactionIndex_MetadataKey:
			return "tx:metadata:" + txIdx.MetadataKey
		}
	}

	if id.account != nil {
		switch acctIdx := id.account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			return "acct:" + acctIdx.Builtin.String()
		case *commonpb.AccountIndex_MetadataKey:
			return "acct:metadata:" + acctIdx.MetadataKey
		}
	}

	if id.logBuiltin != nil {
		return "log:" + id.logBuiltin.String()
	}

	return "unknown"
}

// backfillBBKey builds the bbolt key for persisting backfill progress.
// Format:
//
//	TxBuiltin:    [ledger\x00]b[builtin_byte]
//	TxMetadata:   [ledger\x00]T[key]
//	AcctBuiltin:  [ledger\x00]A[builtin_byte]
//	AcctMetadata: [ledger\x00]a[key]
//	LogBuiltin:   [ledger\x00]l[builtin_byte]
func backfillBBKey(ledger string, id indexID) []byte {
	if id.transaction != nil {
		switch txIdx := id.transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			key := make([]byte, 0, len(ledger)+3)
			key = append(key, ledger...)
			key = append(key, 0x00, readstore.BackfillKindTxBuiltin, byte(txIdx.Builtin))

			return key
		case *commonpb.TransactionIndex_MetadataKey:
			key := make([]byte, 0, len(ledger)+2+len(txIdx.MetadataKey))
			key = append(key, ledger...)
			key = append(key, 0x00, readstore.BackfillKindTxMetadata)
			key = append(key, txIdx.MetadataKey...)

			return key
		}
	}

	if id.account != nil {
		switch acctIdx := id.account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			key := make([]byte, 0, len(ledger)+3)
			key = append(key, ledger...)
			key = append(key, 0x00, readstore.BackfillKindAcctBuiltin, byte(acctIdx.Builtin))

			return key
		case *commonpb.AccountIndex_MetadataKey:
			key := make([]byte, 0, len(ledger)+2+len(acctIdx.MetadataKey))
			key = append(key, ledger...)
			key = append(key, 0x00, readstore.BackfillKindAcctMetadata)
			key = append(key, acctIdx.MetadataKey...)

			return key
		}
	}

	if id.logBuiltin != nil {
		key := make([]byte, 0, len(ledger)+3)
		key = append(key, ledger...)
		key = append(key, 0x00, readstore.BackfillKindLogBuiltin, byte(*id.logBuiltin))

		return key
	}

	return nil
}

// addBackfillTask is a helper that creates a backfill task for the given indexID,
// avoiding duplicates by checking the precomputed bbolt key.
func (b *Builder) addBackfillTask(ledger string, id indexID) {
	bbKey := backfillBBKey(ledger, id)
	for _, t := range b.backfillTasks {
		if string(t.bbKey) == string(bbKey) {
			return
		}
	}

	b.backfillTasks = append(b.backfillTasks, &backfillTask{
		ledger: ledger,
		index:  id,
		cursor: 0,
		bbKey:  bbKey,
	})
}

// addBackfillTaskForTxBuiltin creates a backfill task for a transaction builtin index.
func (b *Builder) addBackfillTaskForTxBuiltin(ledger string, index commonpb.TransactionBuiltinIndex) {
	b.addBackfillTask(ledger, indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_Builtin{Builtin: index},
	}})
}

// addBackfillTaskForTxMetadata creates a backfill task for a transaction metadata index.
func (b *Builder) addBackfillTaskForTxMetadata(ledger string, key string) {
	b.addBackfillTask(ledger, indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: key},
	}})
}

// addBackfillTaskForAcctMetadata creates a backfill task for an account metadata index.
func (b *Builder) addBackfillTaskForAcctMetadata(ledger string, key string) {
	b.addBackfillTask(ledger, indexID{account: &commonpb.AccountIndex{
		Kind: &commonpb.AccountIndex_MetadataKey{MetadataKey: key},
	}})
}

// addBackfillTaskForLogBuiltin creates a backfill task for a log builtin index.
func (b *Builder) addBackfillTaskForLogBuiltin(ledger string, index commonpb.LogBuiltinIndex) {
	b.addBackfillTask(ledger, indexID{logBuiltin: &index})
}

// removeBackfillTask removes a backfill task by index ID and deletes its
// progress from bbolt.
func (b *Builder) removeBackfillTask(id indexID) {
	for i, t := range b.backfillTasks {
		if matchesBackfillIndex(t.index, id) {
			// Delete persisted progress.
			_ = b.readStore.Update(func(tx *bolt.Tx) error {
				return b.readStore.DeleteBackfillProgress(tx, t.bbKey)
			})
			// Remove from slice (order doesn't matter).
			b.backfillTasks[i] = b.backfillTasks[len(b.backfillTasks)-1]
			b.backfillTasks = b.backfillTasks[:len(b.backfillTasks)-1]

			return
		}
	}
}

// schemaRewriteTask tracks the progress of re-encoding metadata index entries
// when a SetMetadataFieldType schema change occurs. Instead of scanning the
// entire reverse map inline during processLogs (hot path), the rewrite is
// deferred and processed in batches alongside backfill tasks.
type schemaRewriteTask struct {
	ledger     string
	targetType commonpb.TargetType   // account or transaction
	key        string                // metadata field name
	toType     commonpb.MetadataType // target type
	rmapCursor []byte                // last reverse map key processed (nil = start)
	bbKey      []byte                // precomputed bbolt key for persistence

	lastProgressLog time.Time
	processedCount  uint64
}

// schemaRewriteBBKey builds the bbolt key for persisting schema rewrite progress.
// Format: [ledger\x00]S[targetType_byte][key].
func schemaRewriteBBKey(ledger string, targetType commonpb.TargetType, key string) []byte {
	bbKey := make([]byte, 0, len(ledger)+3+len(key))
	bbKey = append(bbKey, ledger...)
	bbKey = append(bbKey, 0x00, readstore.BackfillKindSchemaRewrite, byte(targetType))
	bbKey = append(bbKey, key...)

	return bbKey
}

// addSchemaRewriteTask creates a deferred schema rewrite task for a SetMetadataFieldType
// log entry, avoiding duplicates.
func (b *Builder) addSchemaRewriteTask(cfg *ledgerIndexConfig, ledger string, smft *commonpb.SetMetadataFieldTypeLog) {
	// Only rewrite if this metadata key is indexed.
	if !cfg.isMetadataIndexed(smft.GetTargetType(), smft.GetKey()) {
		return
	}

	bbKey := schemaRewriteBBKey(ledger, smft.GetTargetType(), smft.GetKey())
	for _, t := range b.schemaRewriteTasks {
		if string(t.bbKey) == string(bbKey) {
			// Type changed again while a rewrite is in flight — restart
			// from scratch since already-processed entries used the old type.
			t.toType = smft.GetType()
			t.rmapCursor = nil
			t.processedCount = 0

			return
		}
	}

	b.schemaRewriteTasks = append(b.schemaRewriteTasks, &schemaRewriteTask{
		ledger:     ledger,
		targetType: smft.GetTargetType(),
		key:        smft.GetKey(),
		toType:     smft.GetType(),
		bbKey:      bbKey,
	})
}

// removeSchemaRewriteTask removes a schema rewrite task and deletes its progress from bbolt.
func (b *Builder) removeSchemaRewriteTask(idx int) {
	task := b.schemaRewriteTasks[idx]

	_ = b.readStore.Update(func(tx *bolt.Tx) error {
		return b.readStore.DeleteBackfillProgress(tx, task.bbKey)
	})

	b.schemaRewriteTasks[idx] = b.schemaRewriteTasks[len(b.schemaRewriteTasks)-1]
	b.schemaRewriteTasks = b.schemaRewriteTasks[:len(b.schemaRewriteTasks)-1]
}

// processSchemaRewrite processes a batch of reverse map entries for a schema rewrite task.
// Returns true when the rewrite is complete (no more entries to process).
func (b *Builder) processSchemaRewrite(task *schemaRewriteTask, maxEntries int) (bool, error) {
	var ns string

	switch task.targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		ns = readstore.NamespaceAccount
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		ns = readstore.NamespaceTransaction
	default:
		return true, nil
	}

	done := false

	err := b.readStore.Update(func(tx *bolt.Tx) error {
		midxBucket := tx.Bucket(readstore.BucketMetadataIndex)
		eidxBucket := tx.Bucket(readstore.BucketEntityExists)
		rmapBucket := tx.Bucket(readstore.BucketReverseMap)

		kb := dal.NewKeyBuilder()

		rmapPrefix := kb.Reset().
			PutLedgerName(task.ledger).
			PutNamespace(ns).
			Snapshot()

		rc := rmapBucket.Cursor()

		var k, v []byte
		if len(task.rmapCursor) > 0 {
			// Seek to cursor position, then advance past it (we already processed it).
			k, v = rc.Seek(task.rmapCursor)
			if k != nil && string(k) == string(task.rmapCursor) {
				k, v = rc.Next()
			}
		} else {
			k, v = rc.Seek(rmapPrefix)
		}

		processed := 0
		var lastKey []byte

		for ; k != nil && readstore.HasPrefix(k, rmapPrefix); k, v = rc.Next() {
			if processed >= maxEntries {
				break
			}

			metaKey := extractMetadataKeyFromReverseMap(k, rmapPrefix, ns)
			if metaKey != task.key {
				continue
			}

			entityID := extractEntityIDFromReverseMap(k, rmapPrefix, ns)

			// Decode old MetadataValue.
			oldMV := &commonpb.MetadataValue{}
			if err := oldMV.UnmarshalVT(v); err != nil {
				// Skip corrupt entries.
				processed++
				lastKey = cloneBytes(k)

				continue
			}

			// Delete old forward index entry.
			oldEncoded := readstore.EncodeMetadataValue(nil, oldMV)
			oldKey := readstore.MetadataIndexKey(kb, task.ledger, ns, task.key, oldEncoded, entityID)

			if err := midxBucket.Delete(oldKey); err != nil {
				return err
			}

			// Convert to new type.
			newMV := commonpb.ConvertMetadataValue(oldMV, task.toType)
			newEncoded := readstore.EncodeMetadataValue(nil, newMV)

			// Update eidx if null status changed.
			oldIsNull := len(oldEncoded) > 0 && oldEncoded[0] == readstore.TypeTagNull
			newIsNull := len(newEncoded) > 0 && newEncoded[0] == readstore.TypeTagNull

			if oldIsNull != newIsNull {
				oldEidxKey := readstore.EntityExistsKey(kb, task.ledger, ns, task.key, oldIsNull, entityID)
				if err := eidxBucket.Delete(oldEidxKey); err != nil {
					return err
				}

				newEidxKey := readstore.EntityExistsKey(kb, task.ledger, ns, task.key, newIsNull, entityID)
				if err := eidxBucket.Put(newEidxKey, nil); err != nil {
					return err
				}
			}

			// Write new forward index entry.
			newKey := readstore.MetadataIndexKey(kb, task.ledger, ns, task.key, newEncoded, entityID)
			if err := midxBucket.Put(newKey, nil); err != nil {
				return err
			}

			// Update reverse map with new value.
			newMVBytes, err := newMV.MarshalVT()
			if err != nil {
				return err
			}

			if err := rmapBucket.Put(k, newMVBytes); err != nil {
				return err
			}

			processed++
			lastKey = cloneBytes(k)
		}

		// Check if we've exhausted the prefix.
		if k == nil || !readstore.HasPrefix(k, rmapPrefix) {
			// Scan the remaining keys to see if there are any more matching entries.
			// We might have skipped non-matching keys, so we only know we're done
			// if we've scanned past the prefix.
			done = true
		}

		task.processedCount += uint64(processed)

		// Persist cursor.
		if lastKey != nil {
			task.rmapCursor = lastKey
			// Value format: [toType_byte][rmapCursor...]
			val := make([]byte, 1+len(lastKey))
			val[0] = byte(task.toType)
			copy(val[1:], lastKey)

			return b.readStore.WriteBackfillCursor(tx, task.bbKey, val)
		}

		return nil
	})

	return done, err
}

// processBackgroundTasks advances both backfill and schema rewrite tasks using
// round-robin scheduling with a time budget.
// When a backfill catches up to globalCursor, it proposes IndexReady (leader only).
// If the proposal fails (not leader or Raft error), the task is kept and retried.
func (b *Builder) processBackgroundTasks(stop <-chan struct{}, globalCursor uint64) {
	b.processBackfills(stop, globalCursor)
	b.processSchemaRewrites(stop)
}

// processSchemaRewrites advances schema rewrite tasks with a time budget.
func (b *Builder) processSchemaRewrites(stop <-chan struct{}) {
	if len(b.schemaRewriteTasks) == 0 {
		return
	}

	deadline := time.Now().Add(b.backfillBudget)
	maxEntriesPerBatch := 500

	i := 0
	for i < len(b.schemaRewriteTasks) && time.Now().Before(deadline) {
		select {
		case <-stop:
			return
		default:
		}

		task := b.schemaRewriteTasks[i]

		done, err := b.processSchemaRewrite(task, maxEntriesPerBatch)
		if err != nil {
			b.logger.WithFields(map[string]any{
				"ledger": task.ledger,
				"key":    task.key,
				"error":  err,
			}).Errorf("Error processing schema rewrite")

			i++

			continue
		}

		// Periodic progress logging.
		now := time.Now()
		if task.lastProgressLog.IsZero() || now.Sub(task.lastProgressLog) >= 10*time.Second {
			b.logger.WithFields(map[string]any{
				"ledger":    task.ledger,
				"key":       task.key,
				"toType":    task.toType.String(),
				"processed": task.processedCount,
			}).Infof("Schema rewrite progress")

			task.lastProgressLog = now
		}

		if done {
			b.logger.WithFields(map[string]any{
				"ledger":    task.ledger,
				"key":       task.key,
				"toType":    task.toType.String(),
				"processed": task.processedCount,
			}).Infof("Schema rewrite complete")

			b.removeSchemaRewriteTask(i)
			// Don't increment i — next task slid into this position.

			continue
		}

		i++
	}
}

// processBackfills advances backfill tasks using round-robin scheduling with a
// time budget. Each task gets an equal share of the budget per tick, preventing
// starvation when multiple indexes are building concurrently.
// When a backfill catches up to globalCursor, it proposes IndexReady (leader only).
// If the proposal fails (not leader or Raft error), the task is kept and retried.
func (b *Builder) processBackfills(stop <-chan struct{}, globalCursor uint64) {
	if len(b.backfillTasks) == 0 {
		return
	}

	deadline := time.Now().Add(b.backfillBudget)
	perTaskBudget := b.backfillBudget / time.Duration(len(b.backfillTasks))
	tasksProcessed := 0

	for tasksProcessed < len(b.backfillTasks) && time.Now().Before(deadline) {
		select {
		case <-stop:
			return
		default:
		}

		// Wrap around.
		if b.nextBackfillIdx >= len(b.backfillTasks) {
			b.nextBackfillIdx = 0
		}

		task := b.backfillTasks[b.nextBackfillIdx]

		if task.cursor >= globalCursor {
			// Backfill is caught up — propose IndexReady.
			if !b.proposeIndexReady(task) {
				// Proposal failed (not leader or Raft error) — keep the task
				// and retry on the next tick. Log periodically so the operator
				// can see that the backfill is done but waiting for leadership.
				now := time.Now()
				if task.lastProgressLog.IsZero() || now.Sub(task.lastProgressLog) >= 30*time.Second {
					b.logger.WithFields(map[string]any{
						"ledger":       task.ledger,
						"index":        backfillIndexName(task.index),
						"cursor":       task.cursor,
						"globalCursor": globalCursor,
						"isLeader":     b.isLeader != nil && b.isLeader(),
						"hasProposer":  b.proposer != nil,
					}).Infof("Backfill caught up, waiting for leadership to propose IndexReady")

					task.lastProgressLog = now
				}

				b.nextBackfillIdx++
				tasksProcessed++

				continue
			}

			b.logger.WithFields(map[string]any{
				"ledger": task.ledger,
				"index":  backfillIndexName(task.index),
				"cursor": task.cursor,
			}).Infof("Backfill complete, IndexReady proposed")

			// Delete persisted progress.
			_ = b.readStore.Update(func(tx *bolt.Tx) error {
				return b.readStore.DeleteBackfillProgress(tx, task.bbKey)
			})

			// Remove from slice — the next task slides into this position,
			// so don't increment nextBackfillIdx.
			b.backfillTasks[b.nextBackfillIdx] = b.backfillTasks[len(b.backfillTasks)-1]
			b.backfillTasks = b.backfillTasks[:len(b.backfillTasks)-1]

			continue
		}

		// Process this task with its share of the budget.
		taskDeadline := time.Now().Add(perTaskBudget)

		var err error
		if isPostingIndex(task.index) {
			err = b.processBackfillPostings(stop, task, taskDeadline)
		} else {
			err = b.processBackfill(stop, task, taskDeadline)
		}

		if err != nil {
			b.logger.WithFields(map[string]any{
				"ledger": task.ledger,
				"index":  backfillIndexName(task.index),
				"cursor": task.cursor,
				"error":  err,
			}).Errorf("Error processing backfill")
		}

		// Periodic progress logging.
		now := time.Now()
		if task.lastProgressLog.IsZero() || now.Sub(task.lastProgressLog) >= 10*time.Second {
			elapsed := now.Sub(task.lastProgressLog).Seconds()

			var rate float64
			if !task.lastProgressLog.IsZero() && elapsed > 0 {
				rate = float64(task.cursor-task.lastProgressSeq) / elapsed
			}

			var pct float64
			if globalCursor > 0 {
				pct = float64(task.cursor) / float64(globalCursor) * 100
			}

			b.logger.WithFields(map[string]any{
				"ledger": task.ledger,
				"index":  backfillIndexName(task.index),
				"cursor": task.cursor,
				"target": globalCursor,
				"pct":    fmt.Sprintf("%.1f%%", pct),
				"rate":   fmt.Sprintf("%.0f entries/s", rate),
			}).Infof("Backfill progress")

			task.lastProgressLog = now
			task.lastProgressSeq = task.cursor
		}

		b.nextBackfillIdx++
		tasksProcessed++
	}
}

// backfillBatchSize is the number of log entries per bbolt write transaction
// during backfill. Larger than DefaultBatchSize to amortize fsync overhead
// while keeping memory bounded.
const backfillBatchSize = 10_000

// processBackfill reads logs from Pebble using a single iterator and indexes
// them in batches using only the backfilling index's configuration.
// The iterator stays open across batches to avoid repeated NewIter/First
// overhead during catch-up. Processing continues until the deadline is reached
// or EOF. Existence writes are skipped.
func (b *Builder) processBackfill(stop <-chan struct{}, task *backfillTask, deadline time.Time) error {
	logsCursor, err := query.ReadLogsSince(context.Background(), b.pebbleStore, task.cursor, dal.WithReuse(), dal.WithResetFunc(resetLogForReuse))
	if err != nil {
		return err
	}

	defer func() { _ = logsCursor.Close() }()

	// Build a temporary index config with only the backfilling index enabled.
	cfg := b.buildBackfillConfig(task)

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
				log, err := logsCursor.Next()
				if err != nil {
					if errors.Is(err, io.EOF) {
						eof = true

						break
					}

					return err
				}

				// Skip config-mutation log types during backfill.
				if !isDataLog(log) {
					lastSeq = log.GetSequence()
					batchCount++

					continue
				}

				if err := b.indexLogEntry(tx, cfg, log); err != nil {
					return err
				}

				lastSeq = log.GetSequence()
				batchCount++
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

// buildBackfillConfig creates a temporary ledgerIndexConfig containing only
// the backfilling index enabled.
func (b *Builder) buildBackfillConfig(task *backfillTask) *ledgerIndexConfig {
	cfg := newLedgerIndexConfig()

	if task.index.transaction != nil {
		switch txIdx := task.index.transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			cfg.txBuiltinIndexed[txIdx.Builtin] = true
		case *commonpb.TransactionIndex_MetadataKey:
			cfg.txMetadataIndexed[txIdx.MetadataKey] = true
		}
	}

	if task.index.account != nil {
		switch acctIdx := task.index.account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			cfg.acctBuiltinIndexed[acctIdx.Builtin] = true
		case *commonpb.AccountIndex_MetadataKey:
			cfg.acctMetadataIndexed[acctIdx.MetadataKey] = true
		}
	}

	if task.index.logBuiltin != nil {
		cfg.logBuiltinIndexed[*task.index.logBuiltin] = true
	}

	return cfg
}

// isDataLog returns true if the log entry contains indexable data
// (transactions, metadata). Returns false for config-mutation logs
// (CreateIndex, DropIndex, IndexReady, etc.) which must be skipped during backfill.
func isDataLog(log *commonpb.Log) bool {
	if log.GetPayload() == nil {
		return false
	}

	applyLog, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
	if !ok {
		return false
	}

	if applyLog.Apply.GetLog() == nil || applyLog.Apply.GetLog().GetData() == nil {
		return false
	}

	switch applyLog.Apply.GetLog().GetData().GetPayload().(type) {
	case *commonpb.LedgerLogPayload_CreatedTransaction,
		*commonpb.LedgerLogPayload_RevertedTransaction,
		*commonpb.LedgerLogPayload_SavedMetadata,
		*commonpb.LedgerLogPayload_DeletedMetadata:
		return true
	default:
		return false
	}
}

// proposeIndexReady proposes an IndexReady order through Raft (leader only).
// Returns true if the proposal was submitted successfully, false otherwise.
func (b *Builder) proposeIndexReady(task *backfillTask) bool {
	if b.proposer == nil || b.isLeader == nil || !b.isLeader() {
		return false
	}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: task.ledger,
			},
		},
	}

	switch {
	case task.index.transaction != nil:
		order.GetApply().Data = &raftcmdpb.LedgerApplyOrder_IndexReady{
			IndexReady: &raftcmdpb.IndexReadyOrder{
				Index: &raftcmdpb.IndexReadyOrder_Transaction{
					Transaction: task.index.transaction,
				},
			},
		}
	case task.index.account != nil:
		order.GetApply().Data = &raftcmdpb.LedgerApplyOrder_IndexReady{
			IndexReady: &raftcmdpb.IndexReadyOrder{
				Index: &raftcmdpb.IndexReadyOrder_Account{
					Account: task.index.account,
				},
			},
		}
	case task.index.logBuiltin != nil:
		order.GetApply().Data = &raftcmdpb.LedgerApplyOrder_IndexReady{
			IndexReady: &raftcmdpb.IndexReadyOrder{
				Index: &raftcmdpb.IndexReadyOrder_LogBuiltin{
					LogBuiltin: *task.index.logBuiltin,
				},
			},
		}
	}

	if err := b.proposer.ProposeOrders(order); err != nil {
		b.logger.WithFields(map[string]any{
			"ledger": task.ledger,
			"error":  err,
		}).Errorf("Failed to propose IndexReady")

		return false
	}

	return true
}
