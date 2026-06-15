package state

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Recovery owns the Pebble read capability used to hydrate / re-hydrate a
// Machine from the main store (boot, post-follower-sync, leadership-acquired
// dispatch). It is the only type in this package that retains a
// dal.RecoveryReader, so the hot-path Machine receiver has no field or method
// through which it can read Pebble.
//
// The bodies of RecoverState / RestoreCacheFromStore / DispatchMetadataConversionRequests
// live here rather than as private methods on Machine: the code is colocated
// with the capability it uses, and a future contributor reading machine.go
// won't find any read primitive to pull from.
type Recovery struct {
	apply  *Machine
	reader dal.RecoveryReader
}

// NewRecovery wires a Recovery onto an existing Machine. The reader is held
// only on the Recovery, never on the Machine.
func NewRecovery(apply *Machine, reader dal.RecoveryReader) *Recovery {
	return &Recovery{apply: apply, reader: reader}
}

// RecoverState loads all FSM in-memory state from the Pebble data store.
// Called on restart and after follower sync.
func (r *Recovery) RecoverState() error {
	// Create a ReadHandle for functions that need iterator access (PebbleReader).
	// Get-only calls use r.reader directly (PebbleGetter).
	handle, err := r.reader.NewDirectReadHandle()
	if err != nil {
		return fmt.Errorf("creating read handle for recovery: %w", err)
	}

	defer func() { _ = handle.Close() }()

	// Recover lastAppliedIndex from Pebble
	lastAppliedIndex, err := query.ReadLastAppliedIndex(r.reader)
	if err != nil {
		return fmt.Errorf("recovering last applied index: %w", err)
	}

	r.apply.lastAppliedIndex = lastAppliedIndex
	// Route through publishApplied rather than a bare Store: RecoverState also
	// runs at runtime (SynchronizeWithLeader), where a silent advance of
	// lastPersistedIndex would strand any blocked WaitForApplied caller — the
	// lost-wakeup shape of #327.
	r.apply.publishApplied(lastAppliedIndex)

	// Recover nextSequenceID from last log sequence
	lastSeq, err := query.ReadLastSequence(handle)
	if err != nil {
		return fmt.Errorf("recovering last sequence: %w", err)
	}

	if lastSeq > 0 {
		r.apply.nextSequenceID = lastSeq + 1
	}

	// Recover lastAuditHash and nextAuditSequenceID from last audit entry
	lastAuditEntry, err := query.ReadLastAuditEntry(handle)
	if err != nil {
		return fmt.Errorf("recovering last audit entry: %w", err)
	}

	if lastAuditEntry != nil {
		r.apply.lastAuditHash = lastAuditEntry.GetHash()
		r.apply.nextAuditSequenceID = lastAuditEntry.GetSequence() + 1
	}

	// Recover nextQueryCheckpointID from persisted counter
	nextQCPID, err := query.ReadNextQueryCheckpointID(r.reader)
	if err != nil {
		return fmt.Errorf("recovering next query checkpoint ID: %w", err)
	}

	r.apply.nextQueryCheckpointID = nextQCPID

	// Recover query checkpoint schedule
	qcpSchedule, err := query.ReadQueryCheckpointSchedule(r.reader)
	if err != nil {
		return fmt.Errorf("recovering query checkpoint schedule: %w", err)
	}

	r.apply.queryCheckpointSchedule = qcpSchedule

	// Recover lastAppliedTimestamp from Pebble
	lastAppliedTimestamp, err := query.ReadLastAppliedTimestamp(r.reader)
	if err != nil {
		return fmt.Errorf("recovering last applied timestamp: %w", err)
	}

	r.apply.lastAppliedTimestamp = lastAppliedTimestamp

	// Recover periods from Pebble
	periodsCursor, err := query.ReadPeriods(context.Background(), handle)
	if err != nil {
		return fmt.Errorf("recovering periods: %w", err)
	}

	periodsFromStore, err := cursor.Collect(periodsCursor)
	if err != nil {
		return fmt.Errorf("collecting periods: %w", err)
	}

	allPeriods := make(map[uint64]*commonpb.Period, len(periodsFromStore))

	var currentOpenPeriod *commonpb.Period

	var closingPeriods []*commonpb.Period

	for _, p := range periodsFromStore {
		allPeriods[p.GetId()] = p

		switch p.GetStatus() {
		case commonpb.PeriodStatus_PERIOD_OPEN:
			currentOpenPeriod = p
		case commonpb.PeriodStatus_PERIOD_CLOSING:
			closingPeriods = append(closingPeriods, p)
		}
	}

	nextPeriodID, err := query.ReadNextPeriodID(r.reader)
	if err != nil {
		return fmt.Errorf("recovering next period ID: %w", err)
	}

	r.apply.Periods.Reset(allPeriods, currentOpenPeriod, closingPeriods, nextPeriodID)

	// Recover period schedule from Pebble
	periodSchedule, err := query.ReadPeriodSchedule(r.reader)
	if err != nil {
		return fmt.Errorf("recovering period schedule: %w", err)
	}

	r.apply.Periods.SetSchedule(periodSchedule)

	// Recover reversions from Pebble
	reversions, err := query.ReadReversions(handle)
	if err != nil {
		return fmt.Errorf("recovering reversions: %w", err)
	}

	r.apply.Registry.Reversions = reversions

	// Recover pending ledger cleanups from Pebble
	pendingCleanups, err := query.ReadPendingLedgerCleanups(handle)
	if err != nil {
		return fmt.Errorf("recovering pending ledger cleanups: %w", err)
	}

	r.apply.pendingLedgerCleanups = pendingCleanups

	// Recover nextLedgerID from persisted counter
	nextLedgerID, err := query.ReadNextLedgerID(r.reader)
	if err != nil {
		return fmt.Errorf("recovering next ledger ID: %w", err)
	}

	r.apply.nextLedgerID = nextLedgerID

	// Recover signing keys from Pebble
	if r.apply.keyStore != nil {
		r.apply.keyStore.Reset()

		signingKeys, err := query.ReadSigningKeys(handle)
		if err != nil {
			return fmt.Errorf("loading signing keys: %w", err)
		}

		for keyID, entry := range signingKeys {
			r.apply.keyStore.AddPublicKey(keyID, entry.PublicKey, entry.ParentKeyID)
		}
	}

	// Recover shared runtime flags from Pebble
	r.apply.sharedState.Reset()

	requireSig, err := query.ReadSigningConfig(r.reader)
	if err != nil {
		return fmt.Errorf("loading signing config: %w", err)
	}

	r.apply.sharedState.SetRequireSignatures(requireSig)

	maintenanceMode, err := query.ReadMaintenanceMode(r.reader)
	if err != nil {
		return fmt.Errorf("loading maintenance mode: %w", err)
	}

	r.apply.sharedState.SetMaintenanceMode(maintenanceMode)

	clusterState, err := query.ReadClusterState(r.reader)
	if err != nil {
		return fmt.Errorf("loading cluster state: %w", err)
	}

	if clusterState != nil {
		r.apply.lastClusterConfig = clusterState.GetConfig()
		r.apply.Registry.Cache.SetGenerationThreshold(clusterState.GetConfig().GetRotationThreshold())
		// Epoch is never 0 in the running cache (cache.New initializes it to 1).
		// Persisted clusterState from before that change may still carry 0 —
		// bump it up so the staleness check never sees a zero live epoch (#302).
		persistedEpoch := clusterState.GetCacheEpoch()
		if persistedEpoch == 0 {
			persistedEpoch = 1
		}

		r.apply.Registry.Cache.SetEpoch(persistedEpoch)
		r.apply.hashGenerator = processing.NewHashGenerator(clusterState.GetConfig().GetHashAlgorithm(), r.apply.clusterID)
	}

	// Rebuild the idempotency bridge from Pebble. Without this, a node that
	// restarts loses every idempotency key whose surrounding proposal already
	// landed in Pebble — replays would then be accepted as new work until the
	// in-memory bridge naturally refilled. See issue #300.
	r.apply.Registry.Idempotency.Reset()

	if err := r.apply.Registry.Idempotency.RestoreFromStore(handle); err != nil {
		return fmt.Errorf("recovering idempotency bridge: %w", err)
	}

	r.apply.logger.WithFields(map[string]any{
		"nextSequenceID":        r.apply.nextSequenceID,
		"nextAuditSequenceID":   r.apply.nextAuditSequenceID,
		"nextQueryCheckpointID": r.apply.nextQueryCheckpointID,
		"hasAuditHash":          len(r.apply.lastAuditHash) > 0,
		"periodCount":           len(allPeriods),
		"reversionLedgers":      len(reversions),
		"pendingCleanups":       len(pendingCleanups),
	}).Infof("Recovered FSM state from store")

	return nil
}

// RestoreCacheFromStore re-hydrates the in-memory cache from the 0xFF zone in
// Pebble. The cache snapshotter holds its own read capability scoped to
// restore.
func (r *Recovery) RestoreCacheFromStore() error {
	if err := r.apply.cacheSnapshotter.RestoreFromStore(r.reader); err != nil {
		return err
	}

	if r.apply.sentinelMode {
		r.apply.cacheSnapshotter.verifyCacheRestoreCoherence()
	}

	return nil
}

// OnLeadershipAcquired is called when this node becomes the Raft leader. It
// re-dispatches archive and metadata-conversion requests from durable state,
// allowing the new leader to retry work that may have been in flight when the
// previous leader crashed.
func (r *Recovery) OnLeadershipAcquired(stop <-chan struct{}) {
	go r.apply.DispatchArchiveRequests(stop)
	go r.DispatchMetadataConversionRequests(stop)
}

// DispatchBloomRebuilds consumes the Machine's bloom-rebuild signal channel
// and triggers an async bloom populate using Recovery's reader. The Machine
// hot path emits on the channel when a cluster-config change requires a
// rebuild; it does not invoke StartAsyncBloomPopulate directly because it
// does not hold a Pebble reader.
//
// Returns when stop is closed; intended to be run as a goroutine by the
// bootstrap lifecycle (similar to other background dispatchers).
func (r *Recovery) DispatchBloomRebuilds(stop <-chan struct{}) {
	ch := r.apply.BloomRebuildCh()
	for {
		select {
		case <-stop:
			return
		case reason, ok := <-ch:
			if !ok {
				return
			}

			r.apply.cacheSnapshotter.StartAsyncBloomPopulate(r.reader, reason)
		}
	}
}

// DispatchMetadataConversionRequests iterates all ledgers and dispatches
// conversion requests for metadata fields still in CONVERTING status. Held
// on Recovery (not Machine) because it needs a Pebble read handle to scan
// ledger metadata.
func (r *Recovery) DispatchMetadataConversionRequests(stop <-chan struct{}) {
	handle, err := r.reader.NewReadHandle()
	if err != nil {
		r.apply.logger.Errorf("Failed to create read handle for metadata conversion recovery: %v", err)

		return
	}

	defer func() { _ = handle.Close() }()

	cursor, err := query.ReadLedgers(context.Background(), handle)
	if err != nil {
		r.apply.logger.Errorf("Failed to read ledgers for metadata conversion recovery: %v", err)

		return
	}

	defer func() { _ = cursor.Close() }()

	for {
		info, err := cursor.Next()
		if err != nil {
			break
		}

		if info.GetMetadataSchema() == nil || info.GetDeletedAt() != nil {
			continue
		}

		r.apply.dispatchConvertingFields(stop, info, commonpb.TargetType_TARGET_TYPE_ACCOUNT, info.GetMetadataSchema().GetAccountFields())
		r.apply.dispatchConvertingFields(stop, info, commonpb.TargetType_TARGET_TYPE_TRANSACTION, info.GetMetadataSchema().GetTransactionFields())
	}
}
