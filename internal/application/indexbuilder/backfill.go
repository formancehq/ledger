package indexbuilder

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// backfillTask tracks the progress of backfilling a single index.
type backfillTask struct {
	ledger   string // ledger name (for logging and config lookup)
	ledgerID uint32 // ledger ID (for BB key construction and readstore keys)
	index    indexID
	cursor   uint64 // current position (persisted in Pebble)
	bbKey    []byte // precomputed key for progress persistence

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

// backfillBBKey builds the key for persisting backfill progress.
// Format:
//
//	TxBuiltin:    [ledgerID_BE_4B]b[builtin_byte]
//	TxMetadata:   [ledgerID_BE_4B]T[key]
//	AcctBuiltin:  [ledgerID_BE_4B]A[builtin_byte]
//	AcctMetadata: [ledgerID_BE_4B]a[key]
//	LogBuiltin:   [ledgerID_BE_4B]l[builtin_byte]
func backfillBBKey(ledgerID uint32, id indexID) []byte {
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], ledgerID)

	if id.transaction != nil {
		switch txIdx := id.transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			key := make([]byte, 0, 4+2)
			key = append(key, prefix[:]...)
			key = append(key, readstore.BackfillKindTxBuiltin, byte(txIdx.Builtin))

			return key
		case *commonpb.TransactionIndex_MetadataKey:
			key := make([]byte, 0, 4+1+len(txIdx.MetadataKey))
			key = append(key, prefix[:]...)
			key = append(key, readstore.BackfillKindTxMetadata)
			key = append(key, txIdx.MetadataKey...)

			return key
		}
	}

	if id.account != nil {
		switch acctIdx := id.account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			key := make([]byte, 0, 4+2)
			key = append(key, prefix[:]...)
			key = append(key, readstore.BackfillKindAcctBuiltin, byte(acctIdx.Builtin))

			return key
		case *commonpb.AccountIndex_MetadataKey:
			key := make([]byte, 0, 4+1+len(acctIdx.MetadataKey))
			key = append(key, prefix[:]...)
			key = append(key, readstore.BackfillKindAcctMetadata)
			key = append(key, acctIdx.MetadataKey...)

			return key
		}
	}

	if id.logBuiltin != nil {
		key := make([]byte, 0, 4+2)
		key = append(key, prefix[:]...)
		key = append(key, readstore.BackfillKindLogBuiltin, byte(*id.logBuiltin))

		return key
	}

	return nil
}

// addBackfillTask is a helper that creates a backfill task for the given indexID,
// avoiding duplicates by checking the precomputed progress key.
func (b *Builder) addBackfillTask(ledgerName string, ledgerID uint32, id indexID) {
	bbKey := backfillBBKey(ledgerID, id)
	for _, t := range b.backfillTasks {
		if string(t.bbKey) == string(bbKey) {
			return
		}
	}

	b.backfillTasks = append(b.backfillTasks, &backfillTask{
		ledger:   ledgerName,
		ledgerID: ledgerID,
		index:    id,
		cursor:   0,
		bbKey:    bbKey,
	})
}

// addBackfillTaskForTxBuiltin creates a backfill task for a transaction builtin index.
func (b *Builder) addBackfillTaskForTxBuiltin(ledgerName string, ledgerID uint32, index commonpb.TransactionBuiltinIndex) {
	b.addBackfillTask(ledgerName, ledgerID, indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_Builtin{Builtin: index},
	}})
}

// addBackfillTaskForTxMetadata creates a backfill task for a transaction metadata index.
func (b *Builder) addBackfillTaskForTxMetadata(ledgerName string, ledgerID uint32, key string) {
	b.addBackfillTask(ledgerName, ledgerID, indexID{transaction: &commonpb.TransactionIndex{
		Kind: &commonpb.TransactionIndex_MetadataKey{MetadataKey: key},
	}})
}

// addBackfillTaskForAcctMetadata creates a backfill task for an account metadata index.
func (b *Builder) addBackfillTaskForAcctMetadata(ledgerName string, ledgerID uint32, key string) {
	b.addBackfillTask(ledgerName, ledgerID, indexID{account: &commonpb.AccountIndex{
		Kind: &commonpb.AccountIndex_MetadataKey{MetadataKey: key},
	}})
}

// addBackfillTaskForLogBuiltin creates a backfill task for a log builtin index.
func (b *Builder) addBackfillTaskForLogBuiltin(ledgerName string, ledgerID uint32, index commonpb.LogBuiltinIndex) {
	b.addBackfillTask(ledgerName, ledgerID, indexID{logBuiltin: &index})
}

// removeBackfillTask removes a backfill task by index ID and deletes its
// persisted progress.
func (b *Builder) removeBackfillTask(id indexID) {
	for i, t := range b.backfillTasks {
		if matchesBackfillIndex(t.index, id) {
			// Delete persisted progress.
			_ = b.readStore.DeleteBackfillProgress(t.bbKey)
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
	ledgerID   uint32
	targetType commonpb.TargetType   // account or transaction
	key        string                // metadata field name
	toType     commonpb.MetadataType // target type
	rmapCursor []byte                // last reverse map key processed (nil = start)
	bbKey      []byte                // precomputed key for persistence

	lastProgressLog time.Time
	processedCount  uint64
}

// schemaRewriteBBKey builds the key for persisting schema rewrite progress.
// Format: [ledgerID_BE_4B]S[targetType_byte][key].
func schemaRewriteBBKey(ledgerID uint32, targetType commonpb.TargetType, key string) []byte {
	bbKey := make([]byte, 0, 4+2+len(key))

	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], ledgerID)

	bbKey = append(bbKey, prefix[:]...)
	bbKey = append(bbKey, readstore.BackfillKindSchemaRewrite, byte(targetType))
	bbKey = append(bbKey, key...)

	return bbKey
}

// addSchemaRewriteTask creates a deferred schema rewrite task for a SetMetadataFieldType
// log entry, avoiding duplicates.
func (b *Builder) addSchemaRewriteTask(cfg *ledgerIndexConfig, ledgerID uint32, smft *commonpb.SetMetadataFieldTypeLog) {
	// Only rewrite if this metadata key is indexed.
	if !cfg.isMetadataIndexed(smft.GetTargetType(), smft.GetKey()) {
		return
	}

	bbKey := schemaRewriteBBKey(ledgerID, smft.GetTargetType(), smft.GetKey())
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
		ledgerID:   ledgerID,
		targetType: smft.GetTargetType(),
		key:        smft.GetKey(),
		toType:     smft.GetType(),
		bbKey:      bbKey,
	})
}

// removeSchemaRewriteTask removes a schema rewrite task and deletes its persisted progress.
func (b *Builder) removeSchemaRewriteTask(idx int) {
	task := b.schemaRewriteTasks[idx]

	_ = b.readStore.DeleteBackfillProgress(task.bbKey)

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

	kb := dal.NewKeyBuilder()

	rmapPrefix := readstore.ReverseMapPrefix(kb, task.ledgerID, ns)
	upper := readstore.IncrementBytes(rmapPrefix)

	// Use a snapshot for the scan so it sees a consistent committed state.
	snap := b.readStore.NewSnapshot()
	defer func() { _ = snap.Close() }()

	var lowerBound []byte
	if len(task.rmapCursor) > 0 {
		lowerBound = task.rmapCursor
	} else {
		lowerBound = rmapPrefix
	}

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: lowerBound,
		UpperBound: upper,
	})
	if err != nil {
		return false, err
	}

	defer func() { _ = iter.Close() }()

	// If resuming from a cursor, seek past it.
	if len(task.rmapCursor) > 0 {
		if !iter.First() {
			done = true

			return done, nil
		}
		// If the first key equals the cursor, skip it (already processed).
		if string(iter.Key()) == string(task.rmapCursor) {
			if !iter.Next() {
				done = true

				return done, nil
			}
		}
	} else if !iter.First() {
		done = true

		return done, nil
	}

	batch := b.readStore.NewBatch()
	defer func() { _ = batch.Cancel() }()

	processed := 0
	var lastKey []byte

	for ; iter.Valid(); iter.Next() {
		if processed >= maxEntries {
			break
		}

		k := iter.Key()

		// Strip the PrefixReverseMap byte for the extract helpers.
		suffixAfterByte := k[1:]
		metaKey := extractMetadataKeyFromReverseMap(suffixAfterByte, rmapPrefix[1:], ns)

		if metaKey != task.key {
			continue
		}

		entityID := extractEntityIDFromReverseMap(suffixAfterByte, rmapPrefix[1:], ns)

		v, verr := iter.ValueAndErr()
		if verr != nil {
			return false, verr
		}

		// Decode old MetadataValue.
		oldMV := &commonpb.MetadataValue{}
		if err := oldMV.UnmarshalVT(v); err != nil {
			// Skip corrupt entries.
			processed++
			lastKey = cloneBytes(k)

			continue
		}

		oldEncoded := readstore.EncodeMetadataValue(nil, oldMV)

		// Convert to new type.
		newMV := commonpb.ConvertMetadataValue(oldMV, task.toType)
		newEncoded := readstore.EncodeMetadataValue(nil, newMV)

		// Delete old forward index entry.
		oldFwdKey := readstore.MetadataIndexKey(kb, task.ledgerID, ns, task.key, oldEncoded, entityID)
		if err := batch.DeleteKey(oldFwdKey); err != nil {
			return false, fmt.Errorf("deleting old forward index: %w", err)
		}

		// Update eidx if null status changed.
		oldIsNull := len(oldEncoded) > 0 && oldEncoded[0] == readstore.TypeTagNull
		newIsNull := len(newEncoded) > 0 && newEncoded[0] == readstore.TypeTagNull

		if oldIsNull != newIsNull {
			oldEidxKey := readstore.EntityExistsKey(kb, task.ledgerID, ns, task.key, oldIsNull, entityID)
			if err := batch.DeleteKey(oldEidxKey); err != nil {
				return false, fmt.Errorf("deleting old eidx: %w", err)
			}

			newEidxKey := readstore.EntityExistsKey(kb, task.ledgerID, ns, task.key, newIsNull, entityID)
			if err := batch.SetBytes(newEidxKey, nil); err != nil {
				return false, fmt.Errorf("setting new eidx: %w", err)
			}
		}

		// Write new forward index entry.
		newFwdKey := readstore.MetadataIndexKey(kb, task.ledgerID, ns, task.key, newEncoded, entityID)
		if err := batch.SetBytes(newFwdKey, nil); err != nil {
			return false, fmt.Errorf("setting new forward index: %w", err)
		}

		// Update reverse map with new encoded value.
		if err := batch.SetBytes(cloneBytes(k), newEncoded); err != nil {
			return false, fmt.Errorf("updating reverse map: %w", err)
		}

		processed++
		lastKey = cloneBytes(k)
	}

	// Check if we've exhausted the prefix.
	if !iter.Valid() {
		done = true
	}

	task.processedCount += uint64(processed)

	// Persist cursor into the same batch.
	if lastKey != nil {
		task.rmapCursor = lastKey
		// Value format: [toType_byte][rmapCursor...]
		val := make([]byte, 1+len(lastKey))
		val[0] = byte(task.toType)
		copy(val[1:], lastKey)

		if err := b.readStore.WriteBackfillCursor(batch, task.bbKey, val); err != nil {
			return false, err
		}
	}

	if err := batch.Commit(); err != nil {
		return false, err
	}

	return done, nil
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
			// Check if the index is already READY (e.g., applied by the FSM
			// from another leader's proposal). If so, skip proposing and just
			// clean up the task. This prevents follower nodes from getting
			// stuck retrying a proposal they can never send.
			if b.isIndexAlreadyReady(task) {
				b.logger.WithFields(map[string]any{
					"ledger": task.ledger,
					"index":  backfillIndexName(task.index),
				}).Infof("Index already READY in Pebble, cleaning up backfill task")

				_ = b.readStore.DeleteBackfillProgress(task.bbKey)
				b.backfillTasks[b.nextBackfillIdx] = b.backfillTasks[len(b.backfillTasks)-1]
				b.backfillTasks = b.backfillTasks[:len(b.backfillTasks)-1]

				continue
			}

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
			_ = b.readStore.DeleteBackfillProgress(task.bbKey)

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

// backfillBatchSize is the number of log entries per Pebble batch commit
// during backfill. Larger than DefaultBatchSize to amortize write overhead
// while keeping memory bounded.
const backfillBatchSize = 10_000

// processBackfill reads logs from Pebble using a single iterator and indexes
// them in batches using only the backfilling index's configuration.
// The iterator stays open across batches to avoid repeated NewIter/First
// overhead during catch-up. Processing continues until the deadline is reached
// or EOF. Existence writes are skipped.
func (b *Builder) processBackfill(stop <-chan struct{}, task *backfillTask, deadline time.Time) error {
	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		return fmt.Errorf("creating read handle for backfill: %w", err)
	}

	defer func() { _ = handle.Close() }()

	logsCursor, err := query.ReadLogsSince(context.Background(), handle, task.cursor, dal.WithReuse(), dal.WithResetFunc(resetLogForReuse))
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

		batch := b.readStore.NewBatch()
		b.wb.Init(batch)

		for batchCount < backfillBatchSize {
			log, err := logsCursor.Next()
			if err != nil {
				if errors.Is(err, io.EOF) {
					eof = true

					break
				}

				_ = batch.Cancel()

				return err
			}

			// Skip config-mutation log types during backfill.
			if !isDataLog(log) {
				lastSeq = log.GetSequence()
				batchCount++

				continue
			}

			if err := b.indexLogEntry(cfg, log); err != nil {
				_ = batch.Cancel()

				return err
			}

			lastSeq = log.GetSequence()
			batchCount++
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

// proposeIndexReady proposes an IndexReadyUpdate through Raft as a direct
// Proposal field (leader only). Returns true if the proposal was submitted
// successfully, false otherwise.
func (b *Builder) proposeIndexReady(task *backfillTask) bool {
	if b.proposer == nil || b.isLeader == nil || !b.isLeader() {
		return false
	}

	update := &raftcmdpb.IndexReadyUpdate{
		Ledger: task.ledger,
	}

	switch {
	case task.index.transaction != nil:
		update.Index = &raftcmdpb.IndexReadyUpdate_Transaction{
			Transaction: task.index.transaction,
		}
	case task.index.account != nil:
		update.Index = &raftcmdpb.IndexReadyUpdate_Account{
			Account: task.index.account,
		}
	case task.index.logBuiltin != nil:
		update.Index = &raftcmdpb.IndexReadyUpdate_LogBuiltin{
			LogBuiltin: *task.index.logBuiltin,
		}
	}

	if err := b.proposer.ProposeProposal(&raftcmdpb.Proposal{
		IndexReadyUpdates: []*raftcmdpb.IndexReadyUpdate{update},
	}); err != nil {
		b.logger.WithFields(map[string]any{
			"ledger": task.ledger,
			"error":  err,
		}).Errorf("Failed to propose IndexReady")

		return false
	}

	return true
}

// isIndexAlreadyReady checks if the index for this backfill task is already
// marked READY in Pebble (e.g. applied by the FSM from another leader's
// proposal). This prevents follower nodes from retrying IndexReady proposals
// forever when no new logs arrive.
func (b *Builder) isIndexAlreadyReady(task *backfillTask) bool {
	info, err := query.GetLedgerByName(context.Background(), b.pebbleStore, task.ledger)
	if err != nil {
		return false // ledger not found or error; assume not ready
	}

	switch {
	case task.index.transaction != nil:
		switch kind := task.index.transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			bi := info.GetBuiltinIndexes()
			if bi == nil {
				return false
			}

			return isBuiltinReady(bi, kind.Builtin)
		case *commonpb.TransactionIndex_MetadataKey:
			return isMetadataIndexReady(info, commonpb.TargetType_TARGET_TYPE_TRANSACTION, kind.MetadataKey)
		}
	case task.index.account != nil:
		if kind, ok := task.index.account.GetKind().(*commonpb.AccountIndex_MetadataKey); ok {
			return isMetadataIndexReady(info, commonpb.TargetType_TARGET_TYPE_ACCOUNT, kind.MetadataKey)
		}
	case task.index.logBuiltin != nil:
		li := info.GetLogBuiltinIndexes()
		if li == nil {
			return false
		}

		return isLogBuiltinReady(li, *task.index.logBuiltin)
	}

	return false
}

func isBuiltinReady(cfg *commonpb.BuiltinIndexConfig, builtin commonpb.TransactionBuiltinIndex) bool {
	switch builtin {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE:
		return cfg.GetReferenceStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
		return cfg.GetTimestampStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS:
		return cfg.GetAddressStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS:
		return cfg.GetSourceAddressStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DEST_ADDRESS:
		return cfg.GetDestAddressStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
		return cfg.GetInsertedAtStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	}

	return false
}

func isLogBuiltinReady(cfg *commonpb.LogBuiltinIndexConfig, builtin commonpb.LogBuiltinIndex) bool {
	switch builtin {
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_LEDGER:
		return cfg.GetLedgerStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	case commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE:
		return cfg.GetDateStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
	}

	return false
}

func isMetadataIndexReady(info *commonpb.LedgerInfo, target commonpb.TargetType, key string) bool {
	if info.GetMetadataSchema() == nil {
		return false
	}

	var fields map[string]*commonpb.MetadataFieldSchema

	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		fields = info.GetMetadataSchema().GetAccountFields()
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		fields = info.GetMetadataSchema().GetTransactionFields()
	}

	if fields == nil {
		return false
	}

	field, ok := fields[key]
	if !ok {
		return false
	}

	return field.GetIndexBuildStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
}
