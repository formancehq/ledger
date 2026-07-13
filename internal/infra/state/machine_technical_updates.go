package state

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/infra/bloom"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// applyTechnicalUpdates applies Proposal-level technical updates that
// bypass the Order/Log system. Each TechnicalUpdate carries its own
// coverage_bits so the scope passed to its handler admits only the keys
// the proposer declared for that single update — symmetric to Order.
// Payloads that read no cache state (EventsSinkUpdate,
// IdempotencyEviction, ClusterConfig, BackupOrder, IncrementalBackupOrder)
// ship with empty bits and never consult their scope.
//
// Reads and attribute-cache writes both flow through `buffer` — the same
// WriteSet that ProcessOrders will use. Attribute mutations queue in
// `buffer.Derived` and reach the cache + Pebble at WriteSet.Merge. Any
// handler error short-circuits the loop before Merge, so no half-written
// tech-update mutations leak into the cache.
//
// Backup lifecycle handlers (BackupOrder / IncrementalBackupOrder) return
// the typed sentinels ErrBackupInProgress / ErrBackupJobNotFound /
// ErrBackupJobIDCollision directly; machine.applyProposal recognises
// those and converts them into ApplyResult.Error so the proposer learns
// about the rejection without an FSM-level abort.
func (fsm *Machine) applyTechnicalUpdates(scopeFactory processing.ScopeFactory, batch *dal.WriteSession, raftIndex uint64, proposal *raftcmdpb.Proposal) error {
	// Preflight the WHOLE technical-update envelope before dispatching a
	// single handler. applyTechnicalUpdates below mutates state as it
	// walks the slice (applyClusterConfig resets the cache, applyBackupOrder
	// touches BackupJobsState, applyMirrorSyncUpdate queues into the
	// WriteSet, …) — so a malformed later update, or an unsupported mixture
	// of orders and technical updates, must be caught FIRST. Otherwise an
	// earlier handler's direct mutation lands before the later update is
	// rejected. See EN-1524.
	if err := preflightTechnicalUpdates(proposal); err != nil {
		return err
	}

	for i, tu := range proposal.GetTechnicalUpdates() {
		scope, scopeErr := scopeFactory.NewScope(tu.GetCoverageBits())
		if scopeErr != nil {
			return fmt.Errorf("building scope for technical_updates[%d]: %w", i, scopeErr)
		}

		switch kind := tu.GetKind().(type) {
		case *raftcmdpb.TechnicalUpdate_ClusterConfig:
			if err := fsm.applyClusterConfig(batch, raftIndex, kind.ClusterConfig); err != nil {
				return fmt.Errorf("applying technical_updates[%d] cluster config: %w", i, err)
			}
		case *raftcmdpb.TechnicalUpdate_EventsSink:
			if err := fsm.applyEventsSinkUpdate(batch, kind.EventsSink); err != nil {
				return fmt.Errorf("applying technical_updates[%d] events sink update: %w", i, err)
			}
		case *raftcmdpb.TechnicalUpdate_MirrorSync:
			if err := fsm.applyMirrorSyncUpdate(scope, fsm.writeSet, kind.MirrorSync); err != nil {
				return fmt.Errorf("applying technical_updates[%d] mirror sync update: %w", i, err)
			}
		case *raftcmdpb.TechnicalUpdate_IdempotencyEviction:
			if err := fsm.applyIdempotencyEviction(batch, kind.IdempotencyEviction); err != nil {
				return fmt.Errorf("applying technical_updates[%d] idempotency eviction: %w", i, err)
			}
		case *raftcmdpb.TechnicalUpdate_BackupOrder:
			if err := fsm.applyBackupOrder(batch, raftIndex, raftcmdpb.BackupKind_BACKUP_KIND_FULL, kind.BackupOrder); err != nil {
				return fmt.Errorf("applying technical_updates[%d] backup order: %w", i, err)
			}
		case *raftcmdpb.TechnicalUpdate_IncrementalBackupOrder:
			if err := fsm.applyIncrementalBackupOrder(batch, raftIndex, kind.IncrementalBackupOrder); err != nil {
				return fmt.Errorf("applying technical_updates[%d] incremental backup order: %w", i, err)
			}
		default:
			// Rolling-upgrade hazard for the EN-1323 cutover: the
			// cluster-wide IndexReady mechanism (oneof field 8) was
			// removed and the field number reserved. New nodes
			// unmarshal a pre-upgrade IndexReadyUpdate proposal into
			// a nil-kind TechnicalUpdate and fall here, while old
			// nodes still in the cluster successfully apply the
			// proposal via the now-deleted applyIndexReady — FSM
			// divergence.
			//
			// Mitigation is operational, not in-code: drain Raft
			// commit-side before rolling the binary upgrade. A clean
			// drain ensures no IndexReadyUpdate sits past the
			// last-applied index of the old replicas, so newcomers
			// never see one. See docs/ops/deployment.md for the
			// upgrade procedure.
			//
			// Returning an error here (rather than silently no-op'ing)
			// is intentional: per CLAUDE.md invariant #7 a truly-
			// impossible case must surface loudly, and an FSM
			// divergence is the only way this arm fires. All-error
			// on every new node is the safer divergence pattern —
			// operators see the failure on upgraded nodes immediately
			// rather than discover stale forward indexes later.
			return fmt.Errorf("technical_updates[%d]: unsupported kind %T (drain Raft commits before upgrading past EN-1323)", i, kind)
		}
	}

	return nil
}

// preflightTechnicalUpdates is a mutation-free pass over the WHOLE
// technical-update envelope, run before applyTechnicalUpdates dispatches
// any handler. It exists because the dispatch loop interleaves validation
// with mutation: a handler mutates state (cache reset, backup-job map,
// WriteSet queue) the instant it is reached, so a malformed later update
// discovered mid-loop would land AFTER an earlier handler already mutated.
// The preflight moves every cheap structural check ahead of the first
// mutation. See EN-1524.
//
// Three classes of check, each mapped to the surfacing the FSM already
// uses for that class:
//
//  1. Coverage bitset well-formedness — validated against the SAME
//     execution plan the handlers' scopes will use, via the shared
//     validateCoverageBits. A bit past the plan slice, a plan with a
//     malformed AttributeID, or an attr_code the FSM does not handle
//     surfaces as *domain.ErrInvalidExecutionPlan — a business rejection
//     (planInvariantDescribable in machine.go recognises it), never an
//     FSM abort, because no mutation has landed yet.
//
//  2. Producer-shape enforcement — the current producers emit exactly one
//     shape each (see the proposal builders in bootstrap/module.go,
//     application/events/emitter.go, application/backup/orchestrator.go,
//     application/mirror/worker.go): (a) a non-MirrorSync technical update
//     is technical-only and is the SOLE update in a zero-order proposal;
//     (b) MirrorSync may coexist with its mirror-ingest order batch but
//     remains the sole technical update in that proposal. A proposal that
//     violates either — multiple technical updates, or non-MirrorSync
//     updates riding alongside orders — is a producer bug, surfaced as
//     *domain.ErrInvalidExecutionPlan (KindInternal) for the same
//     no-mutation reason as (1).
//
//  3. nil / unsupported kind — surfaced FSM-fatally, identical to the
//     dispatch loop's default arm. Per CLAUDE.md invariant #7 and the
//     EN-1323 rolling-upgrade note there, a nil-kind technical update is
//     an FSM-divergence signal (old nodes apply the pre-cutover payload,
//     new nodes see nil), so it must fail loudly on every new node rather
//     than degrade to a per-proposal business rejection.
func preflightTechnicalUpdates(proposal *raftcmdpb.Proposal) error {
	updates := proposal.GetTechnicalUpdates()
	if len(updates) == 0 {
		return nil
	}

	var plans []*raftcmdpb.AttributeCoverage
	if plan := proposal.GetExecutionPlan(); plan != nil {
		plans = plan.GetAttributes()
	}

	// Shape: at most one technical update per proposal. Every current
	// producer emits exactly one; more than one means a producer bug or a
	// forged proposal, and the two-shape contract below (which reasons
	// about "the" technical update) only holds for a single update.
	if len(updates) > 1 {
		return &domain.ErrInvalidExecutionPlan{
			Reason_: fmt.Sprintf("proposal carries %d technical updates; at most one is supported per proposal", len(updates)),
		}
	}

	tu := updates[0]

	// Coverage: validate the update's bitset against the final execution
	// plan without touching any coverage slot (the mutation applyPlans
	// would do). NewScope re-runs the mutating variant per handler; here we
	// only need the reject-before-mutation guarantee.
	if err := validateCoverageBits(plans, tu.GetCoverageBits(), nil); err != nil {
		return fmt.Errorf("technical_updates[0]: %w", err)
	}

	// Kind + order coexistence. MirrorSync is the only kind allowed to ride
	// alongside an order batch; every other kind is technical-only.
	switch tu.GetKind().(type) {
	case *raftcmdpb.TechnicalUpdate_MirrorSync:
		// Allowed with or without orders (data batch vs. error report).
	case *raftcmdpb.TechnicalUpdate_ClusterConfig,
		*raftcmdpb.TechnicalUpdate_EventsSink,
		*raftcmdpb.TechnicalUpdate_IdempotencyEviction,
		*raftcmdpb.TechnicalUpdate_BackupOrder,
		*raftcmdpb.TechnicalUpdate_IncrementalBackupOrder:
		if len(proposal.GetOrders()) != 0 {
			return &domain.ErrInvalidExecutionPlan{
				Reason_: fmt.Sprintf("technical_updates[0]: %T is technical-only and cannot coexist with %d order(s)", tu.GetKind(), len(proposal.GetOrders())),
			}
		}
	default:
		// nil kind (rolling-upgrade IndexReady cutover) or a kind added
		// without a preflight arm. FSM-fatal, matching the dispatch loop's
		// default arm — see that comment and CLAUDE.md invariant #7.
		return fmt.Errorf("technical_updates[0]: unsupported kind %T (drain Raft commits before upgrading past EN-1323)", tu.GetKind())
	}

	return nil
}

// applyClusterConfig handles cluster config updates (Raft-replicated).
// When the rotation threshold changes, the generation boundaries shift and the
// alternating-byte persistence scheme in 0xFF can lose data on even-generation
// skips. Reset the cache and purge 0xFF entirely — the preloader falls back to
// Pebble reads (0xF1) and the cache rebuilds naturally.
func (fsm *Machine) applyClusterConfig(batch *dal.WriteSession, raftIndex uint64, cfg *commonpb.ClusterConfig) error {
	oldThreshold := fsm.Registry.Cache.GenerationThreshold()
	newThreshold := cfg.GetRotationThreshold()

	if newThreshold != oldThreshold {
		fsm.logger.WithFields(map[string]any{
			"oldThreshold": oldThreshold,
			"newThreshold": newThreshold,
			"raftIndex":    raftIndex,
		}).Infof("Applying cluster config change: resetting cache and purging 0xFF")

		// ResetWithThreshold atomically resets caches, bumps the epoch, sets
		// the new threshold, AND realigns currentGeneration + BaseIndex to
		// Gen(raftIndex, newThreshold) — all under the same cache.mu so
		// admission's next snapshot never observes a transient
		// (currentGeneration=0, threshold=new) window that would falsely
		// trip the CacheUnreachable horizon (2+ predicted rotations).
		fsm.Registry.Cache.ResetWithThreshold(newThreshold, raftIndex)

		// Purge both generation byte positions (0 and 1) in the 0xFF cache zone.
		// We can't use a single DeleteRange from [0xFF] to [0xFF+1] because
		// 0xFF+1 overflows to 0x00 as a byte. Instead, purge each gen byte
		// separately using the same pattern as writeCacheRotation.
		for _, genByte := range []byte{0, 1} {
			if err := batch.DeleteRangeNoSync(
				[]byte{dal.ZoneCache, genByte},
				[]byte{dal.ZoneCache, genByte + 1},
			); err != nil {
				return fmt.Errorf("purging cache gen %d: %w", genByte, err)
			}
		}

		// Persist the post-reset in-memory state — CurrentGeneration AND both
		// per-gen BaseIndex sentinels — so a node restart before the next
		// organic rotation reconstructs the same (currentGeneration,
		// BaseIndex.Gen0, BaseIndex.Gen1) tuple CheckRotationNeeded set here.
		// Without this the disk still says currentGeneration=0, RestoreFromStore
		// loads that, and admission's CheckCache re-observes a stale
		// currentGeneration far behind Gen(nextIndex) — falsely tripping the
		// CacheUnreachable horizon until another apply event catches the
		// generation forward.
		postResetGen := fsm.Registry.Cache.CurrentGeneration()
		gen0Byte := byte(postResetGen % 2)
		gen1Byte := byte((postResetGen + 1) % 2)

		if err := batch.SetProto(
			[]byte{dal.ZoneCache, dal.SubCacheMeta},
			&raftcmdpb.CacheSnapshotMeta{CurrentGeneration: postResetGen},
		); err != nil {
			return fmt.Errorf("persisting cache snapshot meta: %w", err)
		}

		if err := batch.SetProto(
			[]byte{dal.ZoneCache, gen0Byte, dal.SubCacheGenMeta},
			&raftcmdpb.CacheGenerationMeta{BaseIndex: fsm.Registry.Cache.BaseIndex.Gen0},
		); err != nil {
			return fmt.Errorf("persisting gen0 meta: %w", err)
		}

		if err := batch.SetProto(
			[]byte{dal.ZoneCache, gen1Byte, dal.SubCacheGenMeta},
			&raftcmdpb.CacheGenerationMeta{BaseIndex: fsm.Registry.Cache.BaseIndex.Gen1},
		); err != nil {
			return fmt.Errorf("persisting gen1 meta: %w", err)
		}
	}

	// Check if bloom filter config changed. If so, purge persisted blocks
	// and rebuild filters with new dimensions. The preloader falls back to
	// Pebble Gets while IsReady() returns false.
	if fsm.BloomFilters != nil && !bloom.BloomConfigEqual(cfg, fsm.State.LastClusterConfig) {
		fsm.logger.WithFields(map[string]any{
			"raftIndex": raftIndex,
		}).Infof("Bloom filter config changed: purging blocks and rebuilding")

		// Purge all persisted bloom blocks.
		if err := batch.DeleteRangeNoSync(
			[]byte{dal.ZoneGlobal, dal.SubGlobBloom},
			[]byte{dal.ZoneGlobal, dal.SubGlobBloom + 1},
		); err != nil {
			return fmt.Errorf("purging bloom blocks: %w", err)
		}

		// Rebuild filters with new dimensions (sets IsReady=false).
		fsm.BloomFilters.Rebuild(cfg)

		// Signal the bloom-rebuild dispatcher (owned by Recovery, which holds
		// the Pebble reader) to launch async repopulation from an attribute
		// scan. We do not call StartAsyncBloomPopulate directly because the
		// hot-path Machine does not hold a reader.
		select {
		case fsm.bloomRebuildCh <- "bloom config changed via cluster config":
		default:
			// A rebuild is already pending — the latest reason wins via the
			// next signal opportunity; nothing to do here.
		}
	}

	// Persist the cluster state with the current cache epoch.
	// The epoch is deterministic (incremented only by ResetWithThreshold
	// in the FSM apply path) and must be persisted so that nodes
	// restoring from a checkpoint have the correct epoch.
	if err := saveClusterState(batch, &commonpb.PersistedClusterState{
		Config:     cfg,
		CacheEpoch: fsm.Registry.Cache.Epoch(),
	}); err != nil {
		return fmt.Errorf("saving cluster state: %w", err)
	}

	fsm.State.UpdateClusterConfig(cfg)

	return nil
}

// applyEventsSinkUpdate applies a per-sink cursor and status update. No log entry is produced.
func (fsm *Machine) applyEventsSinkUpdate(batch *dal.WriteSession, update *raftcmdpb.EventsSinkUpdate) error {
	if update.GetCursor() > 0 {
		if err := SetSinkCursor(batch, update.GetSinkName(), update.GetCursor()); err != nil {
			return fmt.Errorf("setting sink cursor: %w", err)
		}
	}

	if update.GetClearError() {
		if err := ClearSinkStatus(batch, update.GetSinkName()); err != nil {
			return fmt.Errorf("clearing sink status: %w", err)
		}
	} else if update.GetError() != nil {
		if err := SetSinkStatus(batch, &commonpb.SinkStatus{
			SinkName: update.GetSinkName(),
			Cursor:   update.GetCursor(),
			Error:    update.GetError(),
		}); err != nil {
			return fmt.Errorf("setting sink status: %w", err)
		}
	}

	return nil
}

// applyMirrorSyncUpdate queues a per-ledger mirror cursor / source-head /
// status update into the WriteSet. The actual Pebble writes happen later
// in buffer.Merge, which only runs when ProcessOrders +
// ValidateTransientVolumes succeed. This gating matters because the
// mirror worker bundles ingest orders + the cursor TU in a single
// proposal (see internal/application/mirror/worker.go): without queuing
// through the WriteSet, a business-rejected order would leave the
// cursor advanced via the failure audit batch and the worker would
// silently skip source logs on the next batch.
//
// No log entry is produced.
func (fsm *Machine) applyMirrorSyncUpdate(scope processing.Scope, buffer *WriteSet, update *raftcmdpb.MirrorSyncUpdate) error {
	ledgerInfo, err := scope.Ledgers().Get(domain.LedgerKey{Name: update.GetLedgerName()})
	if errors.Is(err, domain.ErrNotFound) {
		return nil // ledger may have been deleted — stale update, skip
	}

	if err != nil {
		return fmt.Errorf("loading ledger for mirror sync update: %w", err)
	}

	buffer.QueueMirrorSync(MirrorSyncWrite{
		LedgerName:     ledgerInfo.GetName(),
		Cursor:         update.GetCursor(),
		SourceLogCount: update.GetSourceLogCount(),
		ClearError:     update.GetClearError(),
		Error:          update.GetError(),
	})

	return nil
}

// applyIdempotencyEviction evicts expired idempotency keys. No log entry is produced.
// The key hashes were pre-scanned by the leader and included in the proposal,
// so this method is write-only — no Pebble reads occur.
func (fsm *Machine) applyIdempotencyEviction(batch *dal.WriteSession, eviction *raftcmdpb.IdempotencyEviction) error {
	evicted, err := fsm.Registry.Idempotency.Evict(batch, eviction.GetCutoffMicros(), eviction.GetLastScannedTimeIndexKey(), eviction.GetPebbleKeyHashes())
	if err != nil {
		return fmt.Errorf("evicting idempotency keys: %w", err)
	}

	if evicted > 0 {
		fsm.logger.Infof("Evicted %d expired idempotency keys (cutoff=%d)", evicted, eviction.GetCutoffMicros())
	}

	return nil
}
