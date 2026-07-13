package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// --- Producer-shape helpers ------------------------------------------------
//
// Each helper mirrors exactly one producer's emitted proposal shape (see the
// proposal builders referenced in each comment). If a producer's shape drifts,
// the corresponding accept-test below must be updated in lock-step — that is
// the point: these tests pin the shape contract preflightTechnicalUpdates
// enforces.

// clusterConfigProposal mirrors bootstrap/module.go proposeClusterConfigUpdate:
// zero orders, exactly one ClusterConfig technical update, empty coverage bits.
func clusterConfigProposal() *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{
		Date: &commonpb.Timestamp{Data: 1700000000},
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{
				ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: 1000},
			},
		}},
	}
}

// sinkCursorProposal mirrors application/events/emitter.go proposeSinkUpdateOnce.
func sinkCursorProposal() *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{
		Date: &commonpb.Timestamp{Data: 1700000000},
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_EventsSink{
				EventsSink: &raftcmdpb.EventsSinkUpdate{SinkName: "sink-a", Cursor: 5},
			},
		}},
	}
}

// idempotencyEvictionProposal mirrors bootstrap/module.go's idempotency
// eviction scheduler.
func idempotencyEvictionProposal() *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{
		Date: &commonpb.Timestamp{Data: 1700000000},
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_IdempotencyEviction{
				IdempotencyEviction: &raftcmdpb.IdempotencyEviction{CutoffMicros: 42},
			},
		}},
	}
}

// backupOrderProposal mirrors application/backup/orchestrator.go
// wrapFullBackupOrder.
func backupOrderProposal() *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{
		Date: &commonpb.Timestamp{Data: 1700000000},
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_BackupOrder{
				BackupOrder: &raftcmdpb.BackupOrder{
					Op: &raftcmdpb.BackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{JobId: 1}},
				},
			},
		}},
	}
}

// incrementalBackupOrderProposal mirrors wrapIncrementalBackupOrder.
func incrementalBackupOrderProposal() *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{
		Date: &commonpb.Timestamp{Data: 1700000000},
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_IncrementalBackupOrder{
				IncrementalBackupOrder: &raftcmdpb.IncrementalBackupOrder{
					Op: &raftcmdpb.IncrementalBackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{JobId: 1}},
				},
			},
		}},
	}
}

// mirrorSyncErrorProposal mirrors application/mirror/worker.go reportError:
// a technical-only MirrorSync (no orders).
func mirrorSyncErrorProposal() *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{
		Date: &commonpb.Timestamp{Data: 1700000000},
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{
				MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "ledger-a"},
			},
		}},
	}
}

// mirrorSyncDataProposal mirrors application/mirror/worker.go's data batch:
// N mirror-ingest orders coexisting with exactly one MirrorSync cursor update.
func mirrorSyncDataProposal() *raftcmdpb.Proposal {
	return &raftcmdpb.Proposal{
		Date:   &commonpb.Timestamp{Data: 1700000000},
		Orders: []*raftcmdpb.Order{createTransactionOrder("ledger-a", true, newPosting("world", "acct", "EUR", 10))},
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{
			Kind: &raftcmdpb.TechnicalUpdate_MirrorSync{
				MirrorSync: &raftcmdpb.MirrorSyncUpdate{LedgerName: "ledger-a", Cursor: 7},
			},
		}},
	}
}

// TestPreflightTechnicalUpdates_ProducerShapesAccepted proves every current
// proposal builder emits a shape the preflight accepts. If any of these fail,
// a legitimate producer would be spuriously rejected — that is what the
// per-shape helpers guard against.
func TestPreflightTechnicalUpdates_ProducerShapesAccepted(t *testing.T) {
	t.Parallel()

	cases := map[string]*raftcmdpb.Proposal{
		"cluster config":         clusterConfigProposal(),
		"sink cursor":            sinkCursorProposal(),
		"idempotency eviction":   idempotencyEvictionProposal(),
		"backup order":           backupOrderProposal(),
		"incremental backup":     incrementalBackupOrderProposal(),
		"mirror sync error only": mirrorSyncErrorProposal(),
		"mirror sync + ingest":   mirrorSyncDataProposal(),
	}

	for name, proposal := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			require.NoError(t, preflightTechnicalUpdates(proposal))
		})
	}

	t.Run("no technical updates is a no-op", func(t *testing.T) {
		t.Parallel()

		require.NoError(t, preflightTechnicalUpdates(&raftcmdpb.Proposal{
			Orders: []*raftcmdpb.Order{createLedgerOrder("ledger-a")},
		}))
	})
}

// TestPreflightTechnicalUpdates_ForgedShapesRejected proves the preflight
// rejects the shapes no producer emits: multiple technical updates, a
// technical-only update riding alongside orders, and (as a business error vs.
// FSM-fatal split) an unknown/nil kind.
func TestPreflightTechnicalUpdates_ForgedShapesRejected(t *testing.T) {
	t.Parallel()

	t.Run("multiple technical updates rejected", func(t *testing.T) {
		t.Parallel()

		p := clusterConfigProposal()
		// Forge a second update alongside the legitimate first one.
		p.TechnicalUpdates = append(p.TechnicalUpdates, sinkCursorProposal().GetTechnicalUpdates()...)

		err := preflightTechnicalUpdates(p)
		var invalid *domain.ErrInvalidExecutionPlan
		require.ErrorAs(t, err, &invalid, "multi-update proposal must be a business rejection")
	})

	t.Run("mirror sync alongside another update rejected", func(t *testing.T) {
		t.Parallel()

		p := mirrorSyncDataProposal()
		p.TechnicalUpdates = append(p.TechnicalUpdates, sinkCursorProposal().GetTechnicalUpdates()...)

		err := preflightTechnicalUpdates(p)
		var invalid *domain.ErrInvalidExecutionPlan
		require.ErrorAs(t, err, &invalid)
	})

	for name, factory := range map[string]func() *raftcmdpb.Proposal{
		"cluster config with orders":       clusterConfigProposal,
		"sink cursor with orders":          sinkCursorProposal,
		"idempotency eviction with orders": idempotencyEvictionProposal,
		"backup order with orders":         backupOrderProposal,
		"incremental backup with orders":   incrementalBackupOrderProposal,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			p := factory()
			p.Orders = []*raftcmdpb.Order{createLedgerOrder("ledger-a")}

			err := preflightTechnicalUpdates(p)
			var invalid *domain.ErrInvalidExecutionPlan
			require.ErrorAs(t, err, &invalid, "technical-only update must not coexist with orders")
		})
	}

	t.Run("nil kind is FSM-fatal, not a business error", func(t *testing.T) {
		t.Parallel()

		p := &raftcmdpb.Proposal{
			Date:             &commonpb.Timestamp{Data: 1700000000},
			TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{{}}, // no Kind set
		}

		err := preflightTechnicalUpdates(p)
		require.Error(t, err)
		// A nil kind is the EN-1323 rolling-upgrade divergence signal: it
		// must surface FSM-fatally, NOT degrade to a per-proposal business
		// rejection (planInvariantDescribable must not recognise it).
		require.Nil(t, planInvariantDescribable(err),
			"nil-kind update must stay FSM-fatal")
	})

	t.Run("coverage bits past plans length rejected", func(t *testing.T) {
		t.Parallel()

		p := clusterConfigProposal()
		// No execution plan declared, but a coverage bit flags position 0.
		p.TechnicalUpdates[0].CoverageBits = []byte{0x01}

		err := preflightTechnicalUpdates(p)
		var invalid *domain.ErrInvalidExecutionPlan
		require.ErrorAs(t, err, &invalid, "out-of-range coverage bit must be a business rejection")
	})
}

// TestPreflightTechnicalUpdates_NoMutationBeforeRejection is the core EN-1524
// regression: a forged proposal whose FIRST technical update is a legitimate,
// mutation-heavy ClusterConfig (threshold change → cache reset + epoch bump)
// followed by a SECOND update must be rejected by the preflight BEFORE the
// ClusterConfig handler runs. If the preflight were absent, applyTechnicalUpdates
// would dispatch the ClusterConfig, reset the cache and bump the epoch, and only
// THEN reach the second update — leaving the cache mutated by a proposal that is
// ultimately rejected. We assert the reject arrives with the cache untouched.
func TestPreflightTechnicalUpdates_NoMutationBeforeRejection(t *testing.T) {
	t.Parallel()

	fsm, store, _ := newTestMachine(t)
	ctx := context.Background()

	const defaultThreshold = 1000 // newTestMachine's threshold

	epochBefore := fsm.Registry.Cache.Epoch()
	require.Equal(t, uint64(defaultThreshold), fsm.Registry.Cache.GenerationThreshold())

	// Forge: a threshold-changing ClusterConfig (would reset the cache) as the
	// first update, plus a second sink-cursor update. Two updates violate the
	// one-per-proposal contract, so the preflight rejects the whole envelope.
	forged := &raftcmdpb.Proposal{
		Id:   77,
		Date: &commonpb.Timestamp{Data: 1700000077},
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{
			{
				Kind: &raftcmdpb.TechnicalUpdate_ClusterConfig{
					ClusterConfig: &commonpb.ClusterConfig{RotationThreshold: defaultThreshold + 500},
				},
			},
			{
				Kind: &raftcmdpb.TechnicalUpdate_EventsSink{
					EventsSink: &raftcmdpb.EventsSinkUpdate{SinkName: "sink-a", Cursor: 5},
				},
			},
		},
	}

	result, err := fsm.ApplyEntries(ctx, store, makeEntry(t, 1, forged))
	require.NoError(t, err, "a forged envelope must be a business rejection, never an FSM-fatal apply error")
	require.Len(t, result.Results, 1)
	require.Error(t, result.Results[0].Error, "forged multi-update proposal must be rejected")

	var invalid *domain.ErrInvalidExecutionPlan
	require.ErrorAs(t, result.Results[0].Error, &invalid)

	// The ClusterConfig handler never ran: threshold and epoch are untouched.
	require.Equal(t, uint64(defaultThreshold), fsm.Registry.Cache.GenerationThreshold(),
		"generation threshold must be unchanged — ClusterConfig must not have dispatched")
	require.Equal(t, epochBefore, fsm.Registry.Cache.Epoch(),
		"cache epoch must be unchanged — no cache reset must have landed")
	require.Nil(t, fsm.State.LastClusterConfig,
		"cluster config state must be unchanged — no UpdateClusterConfig must have run")
}

// TestPreflightTechnicalUpdates_BackupNotStartedBeforeRejection proves the same
// no-mutation guarantee for the backup lifecycle: a forged proposal whose first
// update is a legitimate BackupOrder Start (which would occupy the active
// destination slot in BackupJobsState) followed by a second update must be
// rejected without ever registering the backup job.
func TestPreflightTechnicalUpdates_BackupNotStartedBeforeRejection(t *testing.T) {
	t.Parallel()

	fsm, store, _ := newTestMachine(t)
	ctx := context.Background()

	dst := &raftcmdpb.BackupDestination{
		BucketId: "bucket-x",
		Target: &raftcmdpb.BackupDestination_S3{
			S3: &raftcmdpb.S3BackupTarget{Bucket: "ledger-backups", Region: "eu-west-1"},
		},
	}

	forged := &raftcmdpb.Proposal{
		Id:   88,
		Date: &commonpb.Timestamp{Data: 1700000088},
		TechnicalUpdates: []*raftcmdpb.TechnicalUpdate{
			{
				Kind: &raftcmdpb.TechnicalUpdate_BackupOrder{
					BackupOrder: &raftcmdpb.BackupOrder{
						Op: &raftcmdpb.BackupOrder_Start{Start: &raftcmdpb.BackupOrderStart{
							JobId:       42,
							Destination: dst,
						}},
					},
				},
			},
			{
				Kind: &raftcmdpb.TechnicalUpdate_EventsSink{
					EventsSink: &raftcmdpb.EventsSinkUpdate{SinkName: "sink-a", Cursor: 5},
				},
			},
		},
	}

	result, err := fsm.ApplyEntries(ctx, store, makeEntry(t, 1, forged))
	require.NoError(t, err)
	require.Len(t, result.Results, 1)
	require.Error(t, result.Results[0].Error)

	require.Nil(t, fsm.Registry.BackupJobs.ActiveByDestination(dst),
		"backup job must not have been registered — Start must not have dispatched before rejection")
}
