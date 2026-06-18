package indexbuilder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// backfillTask tracks the progress of backfilling a single index.
type backfillTask struct {
	ledger   string // ledger name (used for BB keys, readstore keys, logging)
	index    *commonpb.IndexID
	cursor   uint64 // current position (persisted in Pebble)
	auditSeq uint64 // safe audit resume sequence for transient-account filtering
	bbKey    []byte // precomputed key for progress persistence
	proposed bool   // true after IndexReady has been proposed; awaiting FSM apply

	// Progress logging state.
	lastProgressLog time.Time // last time a progress log was emitted
	lastProgressSeq uint64    // cursor value at last progress log
}

// backfillIndexName returns a human-readable name for a backfill index ID.
func backfillIndexName(id *commonpb.IndexID) string {
	if id == nil {
		return "unknown"
	}

	switch k := id.GetKind().(type) {
	case *commonpb.IndexID_TxBuiltin:
		return "tx:" + k.TxBuiltin.String()
	case *commonpb.IndexID_LogBuiltin:
		return "log:" + k.LogBuiltin.String()
	case *commonpb.IndexID_AccountBuiltin:
		return "acct:" + k.AccountBuiltin.String()
	case *commonpb.IndexID_Metadata:
		switch k.Metadata.GetTarget() {
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			return "acct:metadata:" + k.Metadata.GetKey()
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			return "tx:metadata:" + k.Metadata.GetKey()
		default:
			return "metadata:" + k.Metadata.GetKey()
		}
	}

	return "unknown"
}

// backfillBBKey builds the key for persisting backfill progress.
// Format:
//
//	TxBuiltin:    [ledgerName padded 64B]b[builtin_byte]
//	TxMetadata:   [ledgerName padded 64B]T[key]
//	AcctBuiltin:  [ledgerName padded 64B]A[builtin_byte]
//	AcctMetadata: [ledgerName padded 64B]a[key]
//	LogBuiltin:   [ledgerName padded 64B]l[builtin_byte]
func backfillBBKey(ledgerName string, id *commonpb.IndexID) []byte {
	if id == nil {
		return nil
	}

	var prefix [dal.LedgerNameFixedSize]byte
	copy(prefix[:], ledgerName)

	switch k := id.GetKind().(type) {
	case *commonpb.IndexID_TxBuiltin:
		key := make([]byte, 0, dal.LedgerNameFixedSize+2)
		key = append(key, prefix[:]...)
		key = append(key, readstore.BackfillKindTxBuiltin, byte(k.TxBuiltin))

		return key
	case *commonpb.IndexID_LogBuiltin:
		key := make([]byte, 0, dal.LedgerNameFixedSize+2)
		key = append(key, prefix[:]...)
		key = append(key, readstore.BackfillKindLogBuiltin, byte(k.LogBuiltin))

		return key
	case *commonpb.IndexID_AccountBuiltin:
		key := make([]byte, 0, dal.LedgerNameFixedSize+2)
		key = append(key, prefix[:]...)
		key = append(key, readstore.BackfillKindAcctBuiltin, byte(k.AccountBuiltin))

		return key
	case *commonpb.IndexID_Metadata:
		switch k.Metadata.GetTarget() {
		case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
			key := make([]byte, 0, dal.LedgerNameFixedSize+1+len(k.Metadata.GetKey()))
			key = append(key, prefix[:]...)
			key = append(key, readstore.BackfillKindTxMetadata)
			key = append(key, k.Metadata.GetKey()...)

			return key
		case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
			key := make([]byte, 0, dal.LedgerNameFixedSize+1+len(k.Metadata.GetKey()))
			key = append(key, prefix[:]...)
			key = append(key, readstore.BackfillKindAcctMetadata)
			key = append(key, k.Metadata.GetKey()...)

			return key
		}
	}

	return nil
}

// addBackfillTask is a helper that creates a backfill task for the given IndexID,
// avoiding duplicates by checking the precomputed progress key.
func (b *Builder) addBackfillTask(ledgerName string, id *commonpb.IndexID) {
	bbKey := backfillBBKey(ledgerName, id)
	for _, t := range b.backfillTasks {
		if string(t.bbKey) == string(bbKey) {
			return
		}
	}

	b.backfillTasks = append(b.backfillTasks, &backfillTask{
		ledger: ledgerName,
		index:  id,
		cursor: 0,
		bbKey:  bbKey,
	})
}

// addBackfillTaskForTxBuiltin creates a backfill task for a transaction builtin index.
func (b *Builder) addBackfillTaskForTxBuiltin(ledgerName string, index commonpb.TransactionBuiltinIndex) {
	b.addBackfillTask(ledgerName, indexes.TxBuiltinID(index))
}

// addBackfillTaskForTxMetadata creates a backfill task for a transaction metadata index.
func (b *Builder) addBackfillTaskForTxMetadata(ledgerName string, key string) {
	b.addBackfillTask(ledgerName, indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, key))
}

// addBackfillTaskForAcctMetadata creates a backfill task for an account metadata index.
func (b *Builder) addBackfillTaskForAcctMetadata(ledgerName string, key string) {
	b.addBackfillTask(ledgerName, indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, key))
}

// addBackfillTaskForLogBuiltin creates a backfill task for a log builtin index.
func (b *Builder) addBackfillTaskForLogBuiltin(ledgerName string, index commonpb.LogBuiltinIndex) {
	b.addBackfillTask(ledgerName, indexes.LogBuiltinID(index))
}

// removeBackfillTask removes a backfill task by index ID and deletes its
// persisted progress.
func (b *Builder) removeBackfillTask(id *commonpb.IndexID) {
	for i, t := range b.backfillTasks {
		if indexes.Equal(t.index, id) {
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
	targetType commonpb.TargetType   // account or transaction
	key        string                // metadata field name
	toType     commonpb.MetadataType // target type
	rmapCursor []byte                // last reverse map key processed (nil = start)
	bbKey      []byte                // precomputed key for persistence

	lastProgressLog time.Time
	processedCount  uint64

	// done flips when the reverse-map scan is exhausted. We keep the task in
	// the slice afterwards so the loop can propose IndexReady and confirm the
	// FSM applied it before cleanup — same pattern as backfillTask.
	done     bool
	proposed bool
}

// schemaRewriteBBKey builds the key for persisting schema rewrite progress.
// Format: [ledgerName padded 64B]S[targetType_byte][key].
func schemaRewriteBBKey(ledgerName string, targetType commonpb.TargetType, key string) []byte {
	bbKey := make([]byte, 0, dal.LedgerNameFixedSize+2+len(key))

	var prefix [dal.LedgerNameFixedSize]byte
	copy(prefix[:], ledgerName)

	bbKey = append(bbKey, prefix[:]...)
	bbKey = append(bbKey, readstore.BackfillKindSchemaRewrite, byte(targetType))
	bbKey = append(bbKey, key...)

	return bbKey
}

// addSchemaRewriteTask creates a deferred schema rewrite task for a SetMetadataFieldType
// log entry, avoiding duplicates. The ledger name is captured so the
// follow-up IndexReady proposal can address the right LedgerInfo (the FSM
// keys by name).
func (b *Builder) addSchemaRewriteTask(cfg *ledgerIndexConfig, ledgerName string, smft *commonpb.SetMetadataFieldTypeLog) {
	// Only rewrite if this metadata key is indexed.
	if !cfg.isMetadataIndexed(smft.GetTargetType(), smft.GetKey()) {
		return
	}

	bbKey := schemaRewriteBBKey(ledgerName, smft.GetTargetType(), smft.GetKey())
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
		ledger:     ledgerName,
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
func (b *Builder) processSchemaRewrite(task *schemaRewriteTask, maxEntries int, stop <-chan struct{}, deadline time.Time) (bool, error) {
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

	rmapPrefix := readstore.ReverseMapPrefix(kb, task.ledger, ns)
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

	scanned := 0
	rewritten := 0
	var lastScannedKey []byte

scan:
	for ; iter.Valid(); iter.Next() {
		if scanned >= maxEntries || !time.Now().Before(deadline) {
			break
		}

		k := iter.Key()

		select {
		case <-stop:
			break scan
		default:
		}

		lastScannedKey = cloneBytes(k)
		scanned++

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

		// The reverse map stores values in the sortable EncodeMetadataValue
		// format, not protobuf — decode with the matching decoder. A failure
		// here is fatal so the backfill task is retried; silently advancing
		// `processed` for a corrupt entry would mark progress that never
		// rewrote that index entry.
		oldMV, _, err := readstore.DecodeValue(v)
		if err != nil {
			return false, fmt.Errorf("decoding reverse map value at key %x: %w", k, err)
		}

		oldEncoded := v

		// Convert to new type.
		newMV := commonpb.ConvertMetadataValue(oldMV, task.toType)
		newEncoded := readstore.EncodeMetadataValue(nil, newMV)

		// Delete old forward index entry.
		oldFwdKey := readstore.MetadataIndexKey(kb, task.ledger, ns, task.key, oldEncoded, entityID)
		if err := batch.DeleteKey(oldFwdKey); err != nil {
			return false, fmt.Errorf("deleting old forward index: %w", err)
		}

		// Update eidx if null status changed.
		oldIsNull := len(oldEncoded) > 0 && oldEncoded[0] == readstore.TypeTagNull
		newIsNull := len(newEncoded) > 0 && newEncoded[0] == readstore.TypeTagNull

		if oldIsNull != newIsNull {
			oldEidxKey := readstore.EntityExistsKey(kb, task.ledger, ns, task.key, oldIsNull, entityID)
			if err := batch.DeleteKey(oldEidxKey); err != nil {
				return false, fmt.Errorf("deleting old eidx: %w", err)
			}

			newEidxKey := readstore.EntityExistsKey(kb, task.ledger, ns, task.key, newIsNull, entityID)
			if err := batch.SetBytes(newEidxKey, nil); err != nil {
				return false, fmt.Errorf("setting new eidx: %w", err)
			}
		}

		// Write new forward index entry.
		newFwdKey := readstore.MetadataIndexKey(kb, task.ledger, ns, task.key, newEncoded, entityID)
		if err := batch.SetBytes(newFwdKey, nil); err != nil {
			return false, fmt.Errorf("setting new forward index: %w", err)
		}

		// Update reverse map with new encoded value.
		if err := batch.SetBytes(cloneBytes(k), newEncoded); err != nil {
			return false, fmt.Errorf("updating reverse map: %w", err)
		}

		rewritten++
	}

	// Check if we've exhausted the prefix.
	if !iter.Valid() {
		done = true
	}

	task.processedCount += uint64(rewritten)

	// Persist cursor into the same batch.
	if lastScannedKey != nil {
		task.rmapCursor = lastScannedKey
		// Value format: [toType_byte][rmapCursor...]
		val := make([]byte, 1+len(lastScannedKey))
		val[0] = byte(task.toType)
		copy(val[1:], lastScannedKey)

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
func (b *Builder) processBackgroundTasks(ctx context.Context, stop <-chan struct{}, globalCursor uint64) {
	b.processBackfills(ctx, stop, globalCursor)
	b.processSchemaRewrites(ctx, stop)
}

// processSchemaRewrites advances schema rewrite tasks with a time budget.
//
// A task lifecycle has three phases:
//  1. Rewriting: processSchemaRewrite consumes reverse-map entries in batches.
//  2. Done, waiting for IndexReady to flip the field's IndexBuildStatus back
//     from BUILDING to READY (leader proposes; followers wait).
//  3. Cleanup: once the FSM applies IndexReady on this node, the task is
//     removed.
//
// Phase 2 mirrors processBackfills: without it the index stays BUILDING
// forever after a SetMetadataFieldType on an indexed field (see #275).
func (b *Builder) processSchemaRewrites(ctx context.Context, stop <-chan struct{}) {
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

		// Phase 3: cleanup once the FSM applied IndexReady.
		if task.done && b.isSchemaRewriteIndexReady(ctx, task) {
			b.removeSchemaRewriteTask(i)

			continue
		}

		// Phase 2: rewrite finished, drive IndexReady through Raft. Skip the
		// rewrite step; just keep retrying the proposal until applied.
		if task.done {
			b.tryProposeSchemaRewriteIndexReady(ctx, task)
			i++

			continue
		}

		// Phase 1: still rewriting.
		done, err := b.processSchemaRewrite(task, maxEntriesPerBatch, stop, deadline)
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

			task.done = true
			b.tryProposeSchemaRewriteIndexReady(ctx, task)
		}

		i++
	}
}

// tryProposeSchemaRewriteIndexReady proposes IndexReady on the rewritten
// field (leader only). The flag is reset on leadership loss so a new leader
// can re-propose. Mirrors the backfill flow.
func (b *Builder) tryProposeSchemaRewriteIndexReady(ctx context.Context, task *schemaRewriteTask) {
	if task.proposed && (b.isLeader == nil || !b.isLeader()) {
		task.proposed = false
	}

	if task.proposed {
		return
	}

	if !b.proposeSchemaRewriteIndexReady(ctx, task) {
		return
	}

	task.proposed = true

	b.logger.WithFields(map[string]any{
		"ledger": task.ledger,
		"key":    task.key,
		"toType": task.toType.String(),
	}).Infof("Schema rewrite complete, IndexReady proposed — waiting for FSM apply")
}

// processBackfills advances backfill tasks using round-robin scheduling with a
// time budget. Each task gets an equal share of the budget per tick, preventing
// starvation when multiple indexes are building concurrently.
// When a backfill catches up to globalCursor, it proposes IndexReady (leader only).
// If the proposal fails (not leader or Raft error), the task is kept and retried.
func (b *Builder) processBackfills(ctx context.Context, stop <-chan struct{}, globalCursor uint64) {
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
			if b.isIndexAlreadyReady(ctx, task) {
				b.logger.WithFields(map[string]any{
					"ledger": task.ledger,
					"index":  backfillIndexName(task.index),
				}).Infof("Index already READY in Pebble, cleaning up backfill task")

				_ = b.readStore.DeleteBackfillProgress(task.bbKey)
				b.backfillTasks[b.nextBackfillIdx] = b.backfillTasks[len(b.backfillTasks)-1]
				b.backfillTasks = b.backfillTasks[:len(b.backfillTasks)-1]

				continue
			}

			// Backfill is caught up — propose IndexReady if not already proposed.
			// The task is kept until isIndexAlreadyReady confirms the FSM applied
			// the IndexReady update. This prevents progress deletion before apply,
			// which would lose the backfill state if leadership changes.
			// Reset proposed flag on leadership loss so the task can
			// re-propose on the new leader.
			if task.proposed && (b.isLeader == nil || !b.isLeader()) {
				task.proposed = false
			}

			if !task.proposed {
				if b.proposeIndexReady(ctx, task) {
					task.proposed = true

					b.logger.WithFields(map[string]any{
						"ledger": task.ledger,
						"index":  backfillIndexName(task.index),
						"cursor": task.cursor,
					}).Infof("Backfill complete, IndexReady proposed — waiting for FSM apply")
				} else {
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
				}
			}

			// Whether proposed or not, keep the task and check again next tick.
			// isIndexAlreadyReady (above) will clean up once the FSM applies.
			b.nextBackfillIdx++
			tasksProcessed++

			continue
		}

		// Process this task with its share of the budget.
		taskDeadline := time.Now().Add(perTaskBudget)

		var err error
		if isPostingIndex(task.index) {
			err = b.processBackfillPostings(ctx, stop, task, taskDeadline)
		} else {
			err = b.processBackfill(ctx, stop, task, taskDeadline)
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
func (b *Builder) processBackfill(ctx context.Context, stop <-chan struct{}, task *backfillTask, deadline time.Time) error {
	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		return fmt.Errorf("creating read handle for backfill: %w", err)
	}

	defer func() { _ = handle.Close() }()

	logsCursor, err := query.ReadLogsSince(ctx, handle, task.cursor, dal.WithReuse(), dal.WithResetFunc(resetLogForReuse))
	if err != nil {
		return err
	}

	defer func() { _ = logsCursor.Close() }()

	// Build a temporary index config with only the backfilling index enabled.
	cfg := b.buildBackfillConfig(task)

	var audit *auditSync
	if cfg.indexesPostingAddressMappings() {
		audit, err = newAuditSync(ctx, handle, task.auditSeq)
		if err != nil {
			return fmt.Errorf("creating audit sync for backfill: %w", err)
		}

		defer func() { _ = audit.close() }()
	}

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

			if err := b.indexLogEntry(cfg, log, audit); err != nil {
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
		if audit != nil {
			audit.advanceBefore(lastSeq + 1)
			task.auditSeq = audit.resumeSequence()
		}

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
	if task.index == nil {
		return cfg
	}

	cfg.byCanonical[indexes.Canonical(task.index)] = &commonpb.Index{
		Id:          task.index,
		BuildStatus: commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_BUILDING,
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
// successfully, false otherwise. ctx is the stop-derived context of the
// builder loop — the proposer now waits for FSM apply through proposeTechnical,
// so a non-cancellable context would pin this goroutine on shutdown or
// leadership loss.
func (b *Builder) proposeIndexReady(ctx context.Context, task *backfillTask) bool {
	if b.proposer == nil || b.isLeader == nil || !b.isLeader() {
		return false
	}

	update := &raftcmdpb.IndexReadyUpdate{
		Ledger: task.ledger,
		Id:     task.index,
	}

	if err := b.proposer.Propose(ctx, &raftcmdpb.Proposal{
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

// proposeSchemaRewriteIndexReady proposes an IndexReadyUpdate for a
// schema-rewrite task (leader only). The update shape is the same as a
// backfill IndexReady for the equivalent metadata index, so the FSM applies
// it through the existing applyIndexReady →
// processing.ProcessIndexReadyMetadata path and flips IndexBuildStatus from
// BUILDING back to READY.
func (b *Builder) proposeSchemaRewriteIndexReady(ctx context.Context, task *schemaRewriteTask) bool {
	if b.proposer == nil || b.isLeader == nil || !b.isLeader() {
		return false
	}

	if task.targetType != commonpb.TargetType_TARGET_TYPE_ACCOUNT &&
		task.targetType != commonpb.TargetType_TARGET_TYPE_TRANSACTION {
		// Ledger-target metadata isn't indexed today; nothing to mark ready.
		return true
	}

	// After the Index-first-class refactor every metadata-key index is
	// identified by an IndexID{Metadata: {target, key}}, regardless of
	// whether the target is ACCOUNT or TRANSACTION.
	update := &raftcmdpb.IndexReadyUpdate{
		Ledger: task.ledger,
		Id:     indexes.MetadataID(task.targetType, task.key),
	}

	if err := b.proposer.Propose(ctx, &raftcmdpb.Proposal{
		IndexReadyUpdates: []*raftcmdpb.IndexReadyUpdate{update},
	}); err != nil {
		b.logger.WithFields(map[string]any{
			"ledger": task.ledger,
			"key":    task.key,
			"error":  err,
		}).Errorf("Failed to propose IndexReady for schema rewrite")

		return false
	}

	return true
}

// isSchemaRewriteIndexReady checks if the rewritten index has reached READY
// in Pebble — either because the leader proposed it and the FSM applied here,
// or because another leader did the work. Used to clean up the task on every
// node (followers can't propose, so they wait).
func (b *Builder) isSchemaRewriteIndexReady(ctx context.Context, task *schemaRewriteTask) bool {
	info, err := query.GetLedgerByName(ctx, b.pebbleStore, task.ledger)
	if err != nil {
		return false
	}

	if task.targetType != commonpb.TargetType_TARGET_TYPE_ACCOUNT &&
		task.targetType != commonpb.TargetType_TARGET_TYPE_TRANSACTION {
		// Ledger-target: no index, nothing to wait for. Treat as ready so
		// the task is cleaned up.
		return true
	}

	return indexes.IsReady(info, indexes.MetadataID(task.targetType, task.key))
}

// isIndexAlreadyReady checks if the index for this backfill task is already
// marked READY in Pebble (e.g. applied by the FSM from another leader's
// proposal). This prevents follower nodes from retrying IndexReady proposals
// forever when no new logs arrive.
func (b *Builder) isIndexAlreadyReady(ctx context.Context, task *backfillTask) bool {
	info, err := query.GetLedgerByName(ctx, b.pebbleStore, task.ledger)
	if err != nil {
		return false // ledger not found or error; assume not ready
	}

	return indexes.IsReady(info, task.index)
}
