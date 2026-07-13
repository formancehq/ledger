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
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// backfillTask tracks the progress of backfilling a single index.
type backfillTask struct {
	ledger             string // ledger name (used for BB keys, readstore keys, IndexReady proposal, logging)
	index              *commonpb.IndexID
	cursor             uint64 // current position (persisted in Pebble)
	appliedProposalSeq uint64 // safe AppliedProposal resume sequence for transient-account filtering
	bbKey              []byte // precomputed key for progress persistence

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

// addBackfillTaskForAccountBuiltin creates a backfill task for an account builtin index.
func (b *Builder) addBackfillTaskForAccountBuiltin(ledgerName string, index commonpb.AccountBuiltinIndex) {
	b.addBackfillTask(ledgerName, indexes.AccountBuiltinID(index))
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
//
// The task is removed from the slice once the atomic switch fires.
// In the common case the switch lands in the same batch as the last
// v_pending writes; when the read-store cursor is behind the FSM at
// scan-exhaust time (see requiredIndexedSeq below) the switch is
// deferred to a later tick — the scan-complete flag keeps the task
// alive across ticks until the gate releases it.
type schemaRewriteTask struct {
	ledger     string
	targetType commonpb.TargetType   // account or transaction
	key        string                // metadata field name
	toType     commonpb.MetadataType // target type
	rmapCursor []byte                // last reverse map key processed (nil = start)
	bbKey      []byte                // precomputed key for persistence

	lastProgressLog time.Time
	processedCount  uint64

	// requiredIndexedSeq is the highest FSM log sequence sampled at
	// the start of any batch this task has run. The rewrite reads raw
	// stored values from the FSM Pebble store (via fetchStoredMetadataValue);
	// the FSM is always at least as fresh as the read store, so a value
	// the rewrite encodes can reflect log state newer than what
	// processLogs has ingested into the read index. Holding the atomic
	// switch until `readStore.LastIndexedSequence() >= requiredIndexedSeq`
	// guarantees that when CurrentVersion flips to v_new, every entry in
	// the v_new keyspace corresponds to a log sequence the read index
	// has already processed — preserving the contiguous-prefix invariant
	// every query relies on through min_log_sequence.
	//
	// Sampled per batch (not per task lifetime) so it tracks the
	// freshest FSM state any batch could have observed. Reset on a
	// retype landing on an existing task (addSchemaRewriteTask):
	// the previous high-water mark no longer applies once toType
	// changes.
	requiredIndexedSeq uint64

	// scanComplete flips when the rmap iteration exhausts. After this
	// point processSchemaRewrite skips the scan and only tries the
	// atomic switch + GC; the task survives in the slice until the
	// requiredIndexedSeq gate releases it. Reset on retype.
	scanComplete bool
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
//
// Returns an error if persisting the bumped pending_version (or the
// reset backfill cursor) fails — the caller propagates so the batch
// aborts rather than continuing with a desynced cache vs. read store.
func (b *Builder) addSchemaRewriteTask(cfg *ledgerIndexConfig, ledgerName string, smft *commonpb.SetMetadataFieldTypeLog) error {
	// If a backfill is in flight for the same (ledger, metadata index),
	// reset its cursor to 0 instead of enqueueing a separate
	// schemaRewriteTask. The backfill replays the logs via the
	// per-batch schemaResolver, which reads the current declared_type
	// and writes the forward index under the new type directly — no
	// separate rewrite is needed. This branch runs first because
	// stripBuildingIndexes can temporarily hide the BUILDING index from
	// cfg during initial catch-up: relying solely on cfg.isMetadataIndexed
	// would let a stale backfill resume after a crash-recovered retype.
	//
	// NB: no pending_version bump here. The in-flight backfill targets
	// pending_version already; resetting its cursor restarts it under
	// the new declared_type (via the per-batch schemaResolver) into the
	// same v_pending keyspace. Bumping would orphan whatever the
	// backfill already wrote at the current pending and add a second
	// version with no benefit.
	indexID := indexes.MetadataID(smft.GetTargetType(), smft.GetKey())
	for _, bt := range b.backfillTasks {
		if bt.ledger != ledgerName || !indexes.Equal(bt.index, indexID) {
			continue
		}

		bt.cursor = 0
		bt.appliedProposalSeq = 0
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

		return nil
	}

	// No in-flight backfill: only enqueue a rewrite if the index is
	// actually registered (READY). isMetadataIndexed is nil-safe so an
	// unknown ledger (cfg == nil) short-circuits here.
	if !cfg.isMetadataIndexed(smft.GetTargetType(), smft.GetKey()) {
		return nil
	}

	// Bump pending_version in the in-memory cache and persist it in the
	// current batch. The rewrite task will read pending_version from
	// the cache at processSchemaRewrite time to know its target
	// keyspace, and the live write path will dual-write into it.
	if err := b.bumpPendingVersion(ledgerName, indexID); err != nil {
		return fmt.Errorf("bumping pending_version on retype: %w", err)
	}

	bbKey := schemaRewriteBBKey(ledgerName, smft.GetTargetType(), smft.GetKey())
	for _, t := range b.schemaRewriteTasks {
		if string(t.bbKey) == string(bbKey) {
			// Type changed again while a rewrite is in flight — restart
			// from scratch since already-processed entries used the old
			// type. The previously-targeted v_pending keyspace becomes
			// an orphan; the bumpPendingVersion call above already
			// promoted pending to a fresh version, and purgeOrphan-
			// Versions cleans the abandoned one at the next boot.
			//
			// requiredIndexedSeq + scanComplete must also clear: the
			// high-water mark and "scan exhausted" flag from the
			// previous declared type no longer reflect what this
			// new rewrite will observe. Keeping scanComplete=true
			// across a retype would let the new rewrite skip its
			// scan entirely and switch into v_new without writing
			// any entries.
			t.toType = smft.GetType()
			t.rmapCursor = nil
			t.processedCount = 0
			t.requiredIndexedSeq = 0
			t.scanComplete = false

			return nil
		}
	}

	b.schemaRewriteTasks = append(b.schemaRewriteTasks, &schemaRewriteTask{
		ledger:     ledgerName,
		targetType: smft.GetTargetType(),
		key:        smft.GetKey(),
		toType:     smft.GetType(),
		bbKey:      bbKey,
	})

	return nil
}

// bumpPendingVersion advances the per-replica pending_version for an
// index by 1 (or by max(current,pending)+1 when a rewrite is already
// in flight) and persists the resulting IndexVersionState into the
// current batch. Mirrors the FSM-side increment of
// Index.ForwardEncodingVersion in processSetMetadataFieldType — each
// replica derives its own pending the same way, so the two converge as
// long as every log is seen.
func (b *Builder) bumpPendingVersion(ledgerName string, indexID *commonpb.IndexID) error {
	canonical := indexes.Canonical(indexID)
	current, pending := b.versionFor(ledgerName, canonical)

	base := max(pending, current)

	newState := readstore.IndexVersionState{
		CurrentVersion: current,
		PendingVersion: base + 1,
	}

	batch := b.wb.Batch()
	if batch == nil {
		// addSchemaRewriteTask is only ever called with an active batch
		// (processLogs / indexLogEntry both Init the WriteBatch before
		// dispatching log payloads). A nil batch here means the call
		// site is broken — surface it loudly per CLAUDE.md invariant #7.
		return errors.New("invariant: bumpPendingVersion called without an active write batch")
	}

	if err := b.readStore.WriteIndexVersionState(batch, ledgerName, canonical, newState); err != nil {
		return fmt.Errorf("persisting IndexVersionState: %w", err)
	}

	b.putVersionState(ledgerName, canonical, newState)

	return nil
}

// removeSchemaRewriteTask removes a schema rewrite task and deletes its persisted progress.
func (b *Builder) removeSchemaRewriteTask(idx int) {
	task := b.schemaRewriteTasks[idx]

	_ = b.readStore.DeleteBackfillProgress(task.bbKey)

	b.schemaRewriteTasks[idx] = b.schemaRewriteTasks[len(b.schemaRewriteTasks)-1]
	b.schemaRewriteTasks = b.schemaRewriteTasks[:len(b.schemaRewriteTasks)-1]
}

// scheduleResumedRewrites reconstructs the in-flight rewrite task list
// at boot from the persisted per-replica state. Every IndexVersionState
// entry with pending_version != 0 belonged to a rewrite that had not
// reached the atomic switch on this replica when it stopped; the
// dual-write path was active, and v_pending was being populated. The
// resumed task carries the same target keyspace (read from the cache),
// the rmap cursor and the new declared_type (read from the persisted
// BackfillCursor written at the previous batch), and continues the
// rmap scan from where it left off.
//
// Replaces the legacy stopgap that scheduled a from-scratch backfill
// when ReadAllSchemaRewriteProgress found a leftover cursor — that
// path relied on cluster-wide IndexReady to mark progress and didn't
// know which (current, pending) the local replica was at.
func (b *Builder) scheduleResumedRewrites() {
	for ledgerName, inner := range b.indexVersions {
		cfg := b.ledgerConfig(ledgerName)
		if cfg == nil {
			continue
		}

		for canonical, state := range inner {
			if state.PendingVersion == 0 {
				continue
			}

			idx, ok := cfg.byCanonical[canonical]
			if !ok || idx.GetId() == nil {
				continue
			}

			meta, ok := idx.GetId().GetKind().(*commonpb.IndexID_Metadata)
			if !ok || meta.Metadata == nil {
				// pending_version on a non-metadata index is impossible
				// under the current versioning rules — surface it as
				// invariant-violated rather than silently swallowing.
				b.logger.WithFields(map[string]any{
					"ledger":    ledgerName,
					"canonical": canonical,
				}).Errorf("invariant: IndexVersionState with pending_version != 0 on non-metadata index")

				continue
			}

			target := meta.Metadata.GetTarget()
			key := meta.Metadata.GetKey()

			bbKey := schemaRewriteBBKey(ledgerName, target, key)

			rawCursor, hasCursor := b.readStore.ReadBackfillCursor(bbKey)

			var (
				toType     commonpb.MetadataType
				rmapCursor []byte
				haveType   bool
			)

			if hasCursor && len(rawCursor) >= 1 {
				toType = commonpb.MetadataType(rawCursor[0])
				haveType = true

				if len(rawCursor) > 1 {
					rmapCursor = make([]byte, len(rawCursor)-1)
					copy(rmapCursor, rawCursor[1:])
				}
			}

			// If no persisted cursor (the rewrite had been enqueued
			// but never ran a batch), source toType from the FSM —
			// the latest SetMetadataFieldType committed it as the
			// field's declared type before the pending bump landed.
			if !haveType {
				toType = b.resolveResumedToType(ledgerName, target, key)
			}

			b.schemaRewriteTasks = append(b.schemaRewriteTasks, &schemaRewriteTask{
				ledger:     ledgerName,
				targetType: target,
				key:        key,
				toType:     toType,
				bbKey:      bbKey,
				rmapCursor: rmapCursor,
			})

			b.logger.WithFields(map[string]any{
				"ledger":         ledgerName,
				"key":            key,
				"target":         target.String(),
				"pendingVersion": state.PendingVersion,
				"resumeCursor":   len(rmapCursor),
			}).Infof("Resumed in-flight schema rewrite from persisted state")
		}
	}
}

// resolveResumedToType reads the declared_type for a metadata field
// from the FSM-side LedgerInfo.MetadataSchema. Used at boot when the
// resumed rewrite has no persisted toType byte (because the task was
// enqueued but never ran a batch). Falls back to the zero
// MetadataType (STRING) if the FSM has no entry — pathological but
// safe: the rewrite will treat existing rmap entries as already at
// the declared type, run an idempotent re-encode, and reach the
// atomic switch with no functional damage.
func (b *Builder) resolveResumedToType(ledgerName string, target commonpb.TargetType, key string) commonpb.MetadataType {
	handle, err := b.pebbleStore.NewDirectReadHandle()
	if err != nil {
		return commonpb.MetadataType_METADATA_TYPE_STRING
	}

	defer func() { _ = handle.Close() }()

	info, err := query.GetLedgerByName(context.Background(), handle, ledgerName)
	if err != nil || info == nil || info.GetMetadataSchema() == nil {
		return commonpb.MetadataType_METADATA_TYPE_STRING
	}

	var fields map[string]*commonpb.MetadataFieldSchema

	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		fields = info.GetMetadataSchema().GetAccountFields()
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		fields = info.GetMetadataSchema().GetTransactionFields()
	default:
		return commonpb.MetadataType_METADATA_TYPE_STRING
	}

	if schema, ok := fields[key]; ok {
		return schema.GetType()
	}

	return commonpb.MetadataType_METADATA_TYPE_STRING
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
//
// Versioning: the task targets the v_pending keyspace declared in the
// per-replica IndexVersionState cache (set by addSchemaRewriteTask in
// the same FSM batch that ingested the SetMetadataFieldType log). Live
// dual-writes mirror updates into v_pending as the rewrite progresses;
// v_current entries are left untouched until the atomic switch fires
// on completion.
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

	canonical := indexes.Canonical(indexes.MetadataID(task.targetType, task.key))
	currentVersion, pendingVersion := b.versionFor(task.ledger, canonical)

	if pendingVersion == 0 {
		// The schemaRewriteTask exists but the IndexVersionState cache
		// has no pending_version recorded — addSchemaRewriteTask must
		// have populated it. Surface loudly per CLAUDE.md invariant #7
		// rather than spinning silently: a missing pending here means
		// the rewrite would target the same keyspace as live writes,
		// which can't be right.
		return true, fmt.Errorf("invariant: schemaRewriteTask for %s has no pending_version in the cache", canonical)
	}

	// effectiveCurrentVersion's 0→1 promotion mirrors what the live
	// path uses: a never-built-yet index still has its rmap entries
	// under v=1 (the default the non-V helpers embed).
	if currentVersion == 0 {
		currentVersion = 1
	}

	if currentVersion == pendingVersion {
		// Edge case — task would write into the same keyspace it reads.
		// Treat as already-done so the loop cleans it up.
		return true, nil
	}

	kb := dal.NewKeyBuilder()

	// If a prior batch already exhausted the rmap scan, nothing left to
	// rewrite — only the atomic switch stands between us and the new
	// keyspace. Skip the scan setup (snap + fsmHandle + iter) entirely
	// and try the gated switch directly. This is the second-half of the
	// "scan, then maybe switch later" split forced by the requiredIndexedSeq
	// gate: once the gate fires, the task can finally retire.
	if task.scanComplete {
		return b.tryCommitScanCompleteSwitch(task, kb, ns, canonical, currentVersion, pendingVersion)
	}

	done := false

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

	fsmHandle, err := b.pebbleStore.NewReadHandle()
	if err != nil {
		return false, fmt.Errorf("opening FSM snapshot for schema rewrite: %w", err)
	}

	defer func() { _ = fsmHandle.Close() }()

	// Sample the highest FSM log sequence this batch could read from.
	// fetchStoredMetadataValue below returns FSM-latest values, so the
	// forward entries we'll write may reflect log state newer than what
	// processLogs has ingested into the read store. Hold the atomic
	// switch (further down) until LastIndexedSequence catches up to
	// this watermark — otherwise post-switch the v_new keyspace would
	// serve rows reflecting state ahead of the read-store cursor,
	// breaking the contiguous-prefix invariant min_log_sequence
	// callers rely on.
	//
	// Sampled per batch and accumulated as a max so the gate tracks
	// the freshest FSM state any of this task's batches could have
	// observed.
	if fsmSeq, sampleErr := query.ReadLastSequence(fsmHandle); sampleErr != nil {
		return false, fmt.Errorf("sampling FSM log sequence for schema-rewrite gate: %w", sampleErr)
	} else if fsmSeq > task.requiredIndexedSeq {
		task.requiredIndexedSeq = fsmSeq
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

	// Seek to (or past) the cursor. iterValid tracks whether there's
	// anything left to scan after this seek; an empty / fully-cursored
	// rmap sets iterValid=false → the scan loop is a no-op and the
	// switch path below sees done=true. We deliberately do NOT
	// early-return here any more (pre-F3 the function returned done=true
	// before opening the batch, skipping the seq gate and firing the
	// switch unconditionally on empty rmap).
	iterValid := false
	if len(task.rmapCursor) > 0 {
		if iter.First() {
			iterValid = true
			// If the first key equals the cursor, skip it (already processed).
			if string(iter.Key()) == string(task.rmapCursor) {
				iterValid = iter.Next()
			}
		}
	} else {
		iterValid = iter.First()
	}

	// Bind the WriteBatch to a fresh Pebble batch so the rewrite can
	// use the version-aware helpers (ReplaceMetadataIndexV /
	// DeleteMetadataEntryWithPreviousV) and benefit from the
	// read-your-writes rmap overlay across multiple entities within
	// the same batch. Cancel-on-error / Commit-on-success below
	// guarantees we never leave the batch dangling.
	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	committed := false

	defer func() {
		if !committed {
			_ = batch.Cancel()
			b.wb.Reset()
		}
	}()

	if !iterValid {
		// Nothing to scan in this batch — the rmap is empty for this
		// (ns, key) or the cursor already covers everything. Fall
		// through to the switch path so the seq gate decides whether
		// to retire the task now or defer to a later tick.
		done = true
	}

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

		entityID, metaKey, entryVersion, parsed := parseReverseMapKey(k, rmapPrefix, ns)
		if !parsed || metaKey != task.key {
			continue
		}

		// Skip rmap rows that don't belong to v_current. The rewrite
		// reads from v_current and writes to v_pending; without this
		// filter the iterator would also see v_pending rows already
		// written by us or by live dual-writes, leading to wasted
		// re-processing in the best case and lossy overwrites in the
		// worst.
		if entryVersion != currentVersion {
			continue
		}

		rawValue, lookupErr := b.fetchStoredMetadataValue(fsmHandle, task.ledger, task.targetType, task.key, entityID)
		if lookupErr != nil {
			return false, fmt.Errorf("reading stored metadata from FSM at key %x: %w", k, lookupErr)
		}

		// The v_pending rmap may already hold an entry for this entity
		// (live dual-write touched it between retype and the rewrite).
		// Look that up so we delete the matching forward / eidx rows
		// before writing the rewrite's value — otherwise leftover live
		// entries shadow the rewrite under a stale encoding.
		pendingRmapKey := metadataReverseMapKeyV(kb, task.ledger, task.targetType, entityID, task.key, pendingVersion)

		pendingOldEncoded, lookupErr := b.reverseMapValue(pendingRmapKey)
		if lookupErr != nil {
			return false, fmt.Errorf("reading pending-version reverse map: %w", lookupErr)
		}

		// If the FSM has nothing for this entity (deleted between writes
		// and rewrite), drop the v_pending entry (if any). v_current is
		// left intact — queries that still read v_current keep working
		// against the existing snapshot, and v_current's GC happens
		// when the atomic switch fires.
		if rawValue == nil {
			if pendingOldEncoded != nil {
				if derr := b.wb.DeleteMetadataEntryWithPreviousV(
					kb, pendingRmapKey,
					task.ledger, ns, task.key, pendingVersion,
					pendingOldEncoded, entityID,
				); derr != nil {
					return false, fmt.Errorf("deleting v_pending entry for missing FSM value: %w", derr)
				}
			}

			rewritten++

			continue
		}

		newMV := commonpb.ConvertMetadataValue(rawValue, task.toType)
		newEncoded := readstore.EncodeMetadataValue(nil, newMV)

		// ReplaceMetadataIndexV at v_pending deletes the old v_pending
		// forward/eidx (if any), writes the new ones, and updates the
		// v_pending rmap row. v_current is untouched.
		if err := b.wb.ReplaceMetadataIndexV(
			kb, pendingRmapKey,
			task.ledger, ns, task.key, pendingVersion,
			newEncoded, pendingOldEncoded, entityID,
		); err != nil {
			return false, fmt.Errorf("writing v_pending metadata entry: %w", err)
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

	// Atomic switch: when the rewrite has just finished its last entry,
	// promote v_pending to local_current_version *and* GC the v_old
	// keyspaces in the same batch. Single-batch atomicity means a
	// crash mid-commit leaves the read store either fully pre-switch
	// (v_old serves queries, rewrite resumes) or fully post-switch
	// (v_pending serves queries, v_old is gone). No intermediate
	// state where queries land on a switched current_version while
	// stale v_old keys still sit on disk.
	//
	// The switch is GATED on `LastIndexedSequence >= requiredIndexedSeq`
	// (the FSM watermark accumulated during the scan above). If the
	// read store hasn't caught up yet, freeze the task in the
	// scanComplete state — the next call to processSchemaRewrite will
	// route through tryCommitScanCompleteSwitch and fire the switch
	// once the gate releases. Splitting the switch into its own batch
	// in the laggy case is safe: v_pending is fully populated, v_current
	// keeps serving queries, and the switch itself is still a single
	// atomic batch (WriteIndexVersionState + gcVersionAt together).
	var (
		didSwitch bool
		newState  readstore.IndexVersionState
	)

	if done {
		task.scanComplete = true

		lastSeq, seqErr := b.readStore.LastIndexedSequence()
		if seqErr != nil {
			return false, fmt.Errorf("reading indexer progress for schema-rewrite gate: %w", seqErr)
		}

		if lastSeq < task.requiredIndexedSeq {
			// Read store is behind the FSM we observed — defer the switch.
			// Commit only what's in `batch` (cursor + v_pending writes
			// from this scan iteration). The task survives to the next
			// tick via the scanComplete branch at the top of the function.
			done = false
		} else {
			newState = readstore.IndexVersionState{
				CurrentVersion: pendingVersion,
				PendingVersion: 0,
			}

			if err := b.readStore.WriteIndexVersionState(batch, task.ledger, canonical, newState); err != nil {
				return false, fmt.Errorf("persisting atomic version switch: %w", err)
			}

			if err := b.gcVersionAt(batch, kb, task.ledger, ns, task.key, currentVersion); err != nil {
				return false, fmt.Errorf("gc v_old keyspace: %w", err)
			}

			didSwitch = true
		}
	}

	if err := b.wb.Flush(); err != nil {
		return false, fmt.Errorf("committing schema rewrite batch: %w", err)
	}

	committed = true

	if didSwitch {
		b.putVersionState(task.ledger, canonical, newState)
		b.logger.WithFields(map[string]any{
			"ledger":         task.ledger,
			"key":            task.key,
			"toType":         task.toType.String(),
			"fromVersion":    currentVersion,
			"toVersion":      pendingVersion,
			"gcedVersion":    currentVersion,
			"processedCount": task.processedCount,
		}).Infof("Schema rewrite atomic switch — local_current_version advanced")
	}

	return done, nil
}

// tryCommitScanCompleteSwitch handles the second half of a rewrite
// whose rmap scan exhausted in a prior batch (task.scanComplete=true)
// but whose atomic switch was deferred because the read-store cursor
// hadn't caught up to the FSM log seq the rewrite observed. Each tick
// re-checks the gate; once `LastIndexedSequence() >= task.requiredIndexedSeq`
// it opens a small batch with just `WriteIndexVersionState` +
// `gcVersionAt` and commits — the switch is still a single atomic
// batch, just decoupled from the scan-completion batch.
//
// Returns (true, nil) when the switch committed and the task can be
// retired; (false, nil) when the gate hasn't released yet (keep the
// task alive); (false, err) on commit failure.
func (b *Builder) tryCommitScanCompleteSwitch(
	task *schemaRewriteTask, kb *dal.KeyBuilder, ns, canonical string,
	currentVersion, pendingVersion uint32,
) (bool, error) {
	lastSeq, err := b.readStore.LastIndexedSequence()
	if err != nil {
		return false, fmt.Errorf("reading indexer progress for schema-rewrite gate: %w", err)
	}

	if lastSeq < task.requiredIndexedSeq {
		// Gate still closed — task survives, retry next tick.
		return false, nil
	}

	batch := b.readStore.NewBatch()
	b.initBatch(batch)
	committed := false

	defer func() {
		if !committed {
			_ = batch.Cancel()
			b.wb.Reset()
		}
	}()

	newState := readstore.IndexVersionState{
		CurrentVersion: pendingVersion,
		PendingVersion: 0,
	}

	if err := b.readStore.WriteIndexVersionState(batch, task.ledger, canonical, newState); err != nil {
		return false, fmt.Errorf("persisting atomic version switch: %w", err)
	}

	if err := b.gcVersionAt(batch, kb, task.ledger, ns, task.key, currentVersion); err != nil {
		return false, fmt.Errorf("gc v_old keyspace: %w", err)
	}

	if err := b.wb.Flush(); err != nil {
		return false, fmt.Errorf("committing deferred schema-rewrite switch: %w", err)
	}

	committed = true

	b.putVersionState(task.ledger, canonical, newState)
	b.logger.WithFields(map[string]any{
		"ledger":             task.ledger,
		"key":                task.key,
		"toType":             task.toType.String(),
		"fromVersion":        currentVersion,
		"toVersion":          pendingVersion,
		"gcedVersion":        currentVersion,
		"processedCount":     task.processedCount,
		"requiredIndexedSeq": task.requiredIndexedSeq,
		"lastIndexedSeq":     lastSeq,
	}).Infof("Schema rewrite deferred atomic switch — gate released, local_current_version advanced")

	return true, nil
}

// metadataReverseMapKeyV returns the reverse-map key for an entity at a
// given forward-encoding version. entityID is the raw byte form held by
// the rmap key (account address bytes for accounts, 8-byte BE txID for
// transactions).
func metadataReverseMapKeyV(
	kb *dal.KeyBuilder,
	ledger string,
	target commonpb.TargetType,
	entityID []byte,
	metaKey string,
	version uint32,
) []byte {
	switch target {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		return readstore.AccountReverseMapKeyV(kb, ledger, string(entityID), metaKey, version)
	case commonpb.TargetType_TARGET_TYPE_TRANSACTION:
		if len(entityID) != 8 {
			return nil
		}

		txID := binary.BigEndian.Uint64(entityID)

		return readstore.TransactionReverseMapKeyV(kb, ledger, txID, metaKey, version)
	}

	return nil
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
// A task lives in the slice while the rmap scan is still consuming
// entries. processSchemaRewrite returns done==true on the final batch,
// in which the atomic switch (current ← pending) and v_old GC have
// already landed. We remove the task immediately on that same iteration
// — no cluster-wide IndexReady proposal, no waiting, no Phase 2/3.
// The local replica's IndexVersionState is the source of truth for
// queries; the atomic switch's batch commit is the "rewrite complete"
// signal.
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
			// Backfill caught up to the global indexer cursor. Every index
			// kind (metadata and builtin tx/account/log) uses the same
			// per-replica IndexVersionState, so completeBackfill promotes
			// pending → current locally and then the task is dropped.
			if err := b.completeBackfill(task); err != nil {
				b.logger.WithFields(map[string]any{
					"ledger": task.ledger,
					"index":  backfillIndexName(task.index),
					"error":  err,
				}).Errorf("Failed to finalize backfill")

				b.nextBackfillIdx++
				tasksProcessed++

				continue
			}

			b.logger.WithFields(map[string]any{
				"ledger": task.ledger,
				"index":  backfillIndexName(task.index),
				"cursor": task.cursor,
			}).Infof("Backfill complete — local atomic switch applied")

			_ = b.readStore.DeleteBackfillProgress(task.bbKey)
			b.backfillTasks[b.nextBackfillIdx] = b.backfillTasks[len(b.backfillTasks)-1]
			b.backfillTasks = b.backfillTasks[:len(b.backfillTasks)-1]

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

// completeBackfill finalizes a backfill task that has caught up to the
// global indexer cursor. Performs the local atomic switch
// (current ← pending, pending ← 0) in a fresh batch and updates the
// in-memory cache after commit. Covers both metadata indexes (where
// the dual-write path also targets v_pending) and builtin indexes
// (tx/account/log) — the unified IndexVersionState is the per-replica
// "this index is ready to serve queries" signal that drove away
// from the cluster-wide IndexReady proposal.
//
// Note: there's no v_old GC here because a backfill builds v_pending
// from scratch — there's no v_old to reclaim on this replica. (The
// only versioned predecessor that exists is the never-built v=0
// sentinel, which has no on-disk keyspace.)
func (b *Builder) completeBackfill(task *backfillTask) error {
	canonical := indexes.Canonical(task.index)
	_, pending := b.versionFor(task.ledger, canonical)

	if pending == 0 {
		// No pending version was ever recorded — handleCreatedIndexLog
		// always sets pending=1 on a fresh CreateIndex, so this branch
		// means the cache lost the entry (snapshot install, manual
		// state drop). Surface loudly per the project's invariant rule:
		// the backfill ran but has nowhere to promote into.
		return fmt.Errorf("invariant: backfill complete on %s/%s but IndexVersionState has no pending_version",
			task.ledger, canonical)
	}

	newState := readstore.IndexVersionState{
		CurrentVersion: pending,
		PendingVersion: 0,
	}

	batch := b.readStore.NewBatch()
	if err := b.readStore.WriteIndexVersionState(batch, task.ledger, canonical, newState); err != nil {
		_ = batch.Cancel()

		return fmt.Errorf("persisting backfill atomic switch: %w", err)
	}

	if err := batch.Commit(); err != nil {
		return fmt.Errorf("committing backfill atomic switch: %w", err)
	}

	b.putVersionState(task.ledger, canonical, newState)

	return nil
}

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
		b.initBatch(batch)

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

// isDataLog returns true if the log entry is a real ledger log the
// backfill path must process: transactions, metadata, and OrderSkipped.
// OrderSkipped carries its own log id and date (assigned by
// assignSkipLogIDAndDate in the FSM apply path), so it participates in
// the per-ledger LedgerLogIndex and the log-date builtin index just
// like any other ledger log. Returns false for config-mutation logs
// (CreateIndex, DropIndex, IndexReady, etc.) which the live path
// already applied in-memory and never need re-indexing on backfill.
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
		*commonpb.LedgerLogPayload_DeletedMetadata,
		*commonpb.LedgerLogPayload_OrderSkipped:
		return true
	default:
		return false
	}
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
