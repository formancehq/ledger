package indexbuilder

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// backfillTask tracks the progress of backfilling a single index.
type backfillTask struct {
	ledger             string // ledger name (used for BB keys, readstore keys, logging)
	index              *commonpb.IndexID
	cursor             uint64 // current position (persisted in Pebble)
	appliedProposalSeq uint64 // safe AppliedProposal resume sequence for transient-account filtering
	bbKey              []byte // precomputed key for progress persistence
	proposed           bool   // true after IndexReady has been proposed; awaiting FSM apply

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

// removeBackfillTask removes a backfill task matching (ledger, index ID)
// and deletes its persisted progress. Matching by IndexID alone would
// drop an unrelated ledger's backfill when two ledgers index the same
// metadata key — that ledger's index would then stay BUILDING forever.
func (b *Builder) removeBackfillTask(ledgerName string, id *commonpb.IndexID) {
	for i, t := range b.backfillTasks {
		if t.ledger != ledgerName || !indexes.Equal(t.index, id) {
			continue
		}

		// Delete persisted progress.
		_ = b.readStore.DeleteBackfillProgress(t.bbKey)
		// Remove from slice (order doesn't matter).
		b.backfillTasks[i] = b.backfillTasks[len(b.backfillTasks)-1]
		b.backfillTasks = b.backfillTasks[:len(b.backfillTasks)-1]

		return
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

	// requiredIndexedSeq is the highest log sequence sampled in the FSM
	// snapshot at the start of any batch this task has run (via
	// query.ReadLastSequence). Because the rewrite sources raw values
	// from the FSM Pebble store (Option B in PR #503's plan), which is
	// always at least as fresh as the indexer cursor, the forward entries
	// it writes may reflect a log seq newer than what processLogs has
	// ingested. We must therefore hold IndexReady until
	// readStore.LastIndexedSequence() >= requiredIndexedSeq, so that an
	// index marked READY actually represents a contiguous log prefix.
	// Tracked in the *log sequence* space to match LastIndexedSequence —
	// the Raft applied-index counts technical / config / no-op entries
	// too and would leave the gate stuck whenever Raft is ahead of log
	// seq.
	requiredIndexedSeq uint64

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
	// If a backfill is in flight for the same (ledger, metadata index),
	// reset its cursor to 0 instead of enqueueing a separate
	// schemaRewriteTask. The backfill replays the logs via the
	// per-batch schemaResolver, which reads the current declared_type
	// and writes the forward index under the new type directly — no
	// separate rewrite is needed. This branch runs first because
	// stripBuildingIndexes can temporarily hide the BUILDING index from
	// cfg during initial catch-up: relying solely on cfg.isMetadataIndexed
	// would let a stale backfill resume after a crash-recovered retype.
	indexID := indexes.MetadataID(smft.GetTargetType(), smft.GetKey())
	for _, bt := range b.backfillTasks {
		if bt.ledger != ledgerName || !indexes.Equal(bt.index, indexID) {
			continue
		}

		bt.cursor = 0
		bt.appliedProposalSeq = 0
		bt.proposed = false
		bt.lastProgressSeq = 0
		// If DeleteBackfillProgress fails, a crash before the first
		// post-reset Commit would resume from the stale persisted cursor
		// under the new declared_type — inconsistent encoding. Log it
		// loudly so operators notice; the in-memory cursor=0 still wins
		// for the current process.
		if err := b.readStore.DeleteBackfillProgress(bt.bbKey); err != nil {
			b.logger.WithFields(map[string]any{
				"ledger": bt.ledger,
				"index":  backfillIndexName(bt.index),
				"error":  err,
			}).Errorf("Deleting persisted backfill cursor on retype reset")
		}

		return
	}

	// No in-flight backfill: only enqueue a rewrite if the index is
	// actually registered (READY). isMetadataIndexed is nil-safe so an
	// unknown ledger (cfg == nil) short-circuits here.
	if !cfg.isMetadataIndexed(smft.GetTargetType(), smft.GetKey()) {
		return
	}

	bbKey := schemaRewriteBBKey(ledgerName, smft.GetTargetType(), smft.GetKey())
	for _, t := range b.schemaRewriteTasks {
		if string(t.bbKey) == string(bbKey) {
			// Type changed again while a rewrite is in flight or pending
			// cleanup — restart from scratch since already-processed entries
			// used the old type. `done` and `proposed` must also clear: with
			// either still set, tryProposeSchemaRewriteIndexReady would mark
			// the index ready with entries still encoded under the prior type.
			// requiredIndexedSeq is reset as well — the high-water mark from
			// the previous declared type no longer reflects what this new
			// rewrite will actually observe.
			t.toType = smft.GetType()
			t.rmapCursor = nil
			t.processedCount = 0
			t.done = false
			t.proposed = false
			t.requiredIndexedSeq = 0

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

// removeSchemaRewriteTaskByField cancels any in-flight rewrite task matching
// (ledger, target, key). Called when the schema field is removed: the index
// it would mark READY no longer exists, so the task must be discarded
// instead of looping on isSchemaRewriteIndexReady forever.
func (b *Builder) removeSchemaRewriteTaskByField(ledgerName string, target commonpb.TargetType, key string) {
	for i, t := range b.schemaRewriteTasks {
		if t.ledger == ledgerName && t.targetType == target && t.key == key {
			b.removeSchemaRewriteTask(i)

			return
		}
	}
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

	// Pair two Pebble snapshots: one on the read store (rmap, forward
	// index) and one on the FSM Pebble store (canonical stored values).
	// Both are taken inside processBackgroundTasks, which only runs after
	// processLogs has drained — there are no concurrent FSM writers, so the
	// FSM snapshot is at least as fresh as the rmap snapshot. Sourcing the
	// re-encoded value from the raw stored value (not from the rmap, which
	// may already hold a lossy projection from a previous retype) makes the
	// rewrite a pure function of stored state. Cancel-and-restart on a new
	// SetMetadataFieldType is therefore safe: re-running with a new toType
	// always produces the same result for the same stored value.
	snap := b.readStore.NewSnapshot()
	defer func() { _ = snap.Close() }()

	// Use a point-in-time snapshot of the FSM Pebble store so both the
	// log-seq high-water mark and the per-entity fetchStoredMetadataValue
	// lookups observe the same state. A direct handle would let the FSM
	// commit new logs in between, which means the rewrite could encode a
	// value tied to a log seq strictly higher than the bound we
	// captured — leaving requiredIndexedSeq lower than what the forward
	// actually reflects, and reopening the prefix-violation window the
	// gate is meant to close.
	fsmHandle, err := b.pebbleStore.NewReadHandle()
	if err != nil {
		return false, fmt.Errorf("opening FSM snapshot for schema rewrite: %w", err)
	}

	defer func() { _ = fsmHandle.Close() }()

	// Capture the highest *log sequence* in the FSM snapshot — NOT the
	// Raft applied-index. The indexer's LastIndexedSequence advances by
	// applicative log seq, while Raft applied-index counts technical /
	// config / no-op entries too. Comparing the two would leave the gate
	// stuck whenever Raft is ahead of log seq, even though the indexer
	// has fully caught up to the log prefix we read. A slightly inflated
	// bound is safe (only delays IndexReady) but the dimensions must
	// match.
	fsmLogSeq, err := query.ReadLastSequence(fsmHandle)
	if err != nil {
		return false, fmt.Errorf("reading FSM last log sequence for schema rewrite: %w", err)
	}

	if fsmLogSeq > task.requiredIndexedSeq {
		task.requiredIndexedSeq = fsmLogSeq
	}

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

		// The rmap value is the encoded form currently in the forward
		// index — used to delete the existing entry. The new encoded form
		// is derived from the immutable raw stored value in the FSM, NOT
		// from the rmap (which may be a lossy projection from a prior
		// retype).
		oldEncoded := v

		rawValue, lookupErr := b.fetchStoredMetadataValue(fsmHandle, task.ledger, task.targetType, task.key, entityID)
		if lookupErr != nil {
			return false, fmt.Errorf("reading stored metadata from FSM at key %x: %w", k, lookupErr)
		}

		// If the FSM has nothing for this entity (deleted between writes
		// and rewrite), drop both forward and rmap entries.
		if rawValue == nil {
			oldFwdKey := readstore.MetadataIndexKey(kb, task.ledger, ns, task.key, oldEncoded, entityID)
			if derr := batch.DeleteKey(oldFwdKey); derr != nil {
				return false, fmt.Errorf("deleting forward entry for missing FSM value: %w", derr)
			}

			oldIsNull := len(oldEncoded) > 0 && oldEncoded[0] == readstore.TypeTagNull
			oldEidxKey := readstore.EntityExistsKey(kb, task.ledger, ns, task.key, oldIsNull, entityID)
			if derr := batch.DeleteKey(oldEidxKey); derr != nil {
				return false, fmt.Errorf("deleting eidx for missing FSM value: %w", derr)
			}

			if derr := batch.DeleteKey(cloneBytes(k)); derr != nil {
				return false, fmt.Errorf("deleting rmap entry for missing FSM value: %w", derr)
			}

			rewritten++

			continue
		}

		newMV := commonpb.ConvertMetadataValue(rawValue, task.toType)
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

	// Hold IndexReady until the indexer has ingested every log up to the
	// highest FSM applied-index this rewrite has observed. Without this
	// the index could be marked READY while the forward already reflects
	// logs not yet in the read store, violating the prefix invariant
	// clients rely on through min_log_sequence.
	if task.requiredIndexedSeq > 0 {
		lastSeq, err := b.readStore.LastIndexedSequence()
		if err != nil {
			b.logger.WithFields(map[string]any{
				"ledger": task.ledger,
				"key":    task.key,
				"error":  err,
			}).Errorf("Reading indexer progress before proposing schema-rewrite IndexReady")

			return
		}

		if lastSeq < task.requiredIndexedSeq {
			return
		}
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

	// Per-batch schema resolver shared with the replay below. Without it,
	// historical SavedMetadata/CreatedTransaction logs replayed during a
	// metadata-index backfill would skip the declared-type coercion and
	// the forward index would key entries under the raw client type.
	b.batchSchema = newSchemaResolver(handle, b.attrs)
	defer func() { b.batchSchema = nil }()

	logsCursor, err := query.ReadLogsSince(ctx, handle, task.cursor, dal.WithReuse(), dal.WithResetFunc(resetLogForReuse))
	if err != nil {
		return err
	}

	defer func() { _ = logsCursor.Close() }()

	// Build a temporary index config with only the backfilling index enabled.
	cfg := b.buildBackfillConfig(task)

	var proposals *appliedProposalSync
	if cfg.indexesPostingAddressMappings() {
		proposals, err = newAppliedProposalSync(ctx, handle, task.appliedProposalSeq)
		if err != nil {
			return fmt.Errorf("creating applied proposal sync for backfill: %w", err)
		}

		defer func() { _ = proposals.close() }()
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

			if err := b.indexLogEntry(cfg, log, proposals); err != nil {
				_ = batch.Cancel()

				return err
			}

			lastSeq = log.GetSequence()
			batchCount++
		}

		// AppliedProposal cursor errors set during indexLogEntry must be
		// surfaced BEFORE the batch is flushed: every excludedForLog call
		// in this batch could have stashed an iterErr (coverage mismatch
		// or corrupt proto) and returned an empty exclusion set, in which
		// case the per-log mappings already written into b.wb are
		// incomplete. Committing them would persist account->tx mappings
		// for volumes that should have been excluded.
		if proposals != nil {
			if err := proposals.err(); err != nil {
				_ = batch.Cancel()

				return fmt.Errorf("applied proposal cursor failed: %w", err)
			}
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
		if proposals != nil {
			proposals.advanceBefore(lastSeq + 1)
			if err := proposals.err(); err != nil {
				return fmt.Errorf("applied proposal cursor failed: %w", err)
			}
			task.appliedProposalSeq = proposals.resumeSequence()
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
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_IndexReady{IndexReady: update},
		}},
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
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_IndexReady{IndexReady: update},
		}},
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

// fetchStoredMetadataValue reads the raw stored metadata value from the FSM
// Pebble zone for a given (ledger, target, key, entityID). Returns nil when
// the value is not present (entity deleted, key never set, transaction state
// gone). The schema rewrite uses this as its canonical source so re-encoding
// is a pure function of stored state — independent of what the rmap currently
// holds (which may be a lossy projection from a prior retype).
func (b *Builder) fetchStoredMetadataValue(
	reader dal.PebbleReader,
	ledgerName string,
	targetType commonpb.TargetType,
	key string,
	entityID []byte,
) (*commonpb.MetadataValue, error) {
	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		canonicalKey := domain.MetadataKey{
			AccountKey: domain.AccountKey{
				LedgerName: ledgerName,
				Account:    string(entityID),
			},
			Key: key,
		}.Bytes()

		v, err := b.attrs.Metadata.Get(reader, canonicalKey)
		if errors.Is(err, domain.ErrNotFound) {
			return nil, nil
		}

		if err != nil {
			return nil, err
		}

		return v, nil
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		if len(entityID) != 8 {
			return nil, fmt.Errorf("invalid transaction entityID length %d (want 8)", len(entityID))
		}

		txID := binary.BigEndian.Uint64(entityID)

		canonicalKey := domain.TransactionKey{
			LedgerName: ledgerName,
			ID:         txID,
		}.Bytes()

		state, err := b.attrs.Transaction.Get(reader, canonicalKey)
		if errors.Is(err, domain.ErrNotFound) {
			return nil, nil
		}

		if err != nil {
			return nil, err
		}

		v, ok := state.GetMetadata()[key]
		if !ok || v == nil {
			return nil, nil
		}

		return v, nil
	default:
		return nil, fmt.Errorf("unsupported target type %v", targetType)
	}
}
