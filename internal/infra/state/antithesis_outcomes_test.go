//go:build enable_antithesis_sdk

package state

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/antithesistest"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Run real guards in a fresh process; antithesistest.Emitted carries the
// reason that is the only way to observe them.
func TestAntithesisStateEmission(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, run string
		property  string
		condition bool
		wantHit   bool
	}{
		{"coverage", "TestScope_DeclaredAccessAndRejectedAccess", "admission declared every FSM attribute access", false, true},
		{"commit", "TestPreparedBatchOutcomeFacts", "multi-transaction proposal committed", true, true},
		{"revert", "TestPreparedBatchOutcomeFacts", "transaction revert committed", true, true},
		{"staged transfer rejection", "TestAtomicProposal_StagedTransferDoesNotSurviveLaterRejection", "atomic proposal rejected after staging an earlier transaction", true, true},
		{"staged revert rejection", "TestAtomicProposal_StagedRevertDoesNotSurviveLaterRejection", "atomic proposal rejected after staging an earlier transaction", true, true},
		{"first order rejection", "TestApplyProposal_PerProposalIdempotency", "atomic proposal rejected after staging an earlier transaction", true, false},
		{"prepare only", "TestPreparedBatchWithoutCommit", "multi-transaction proposal committed", true, false},
		{"sentinel enabled", "TestSentinelOutcome/enabled", "nonempty sentinel verification completed", true, true},
		{"sentinel disabled", "TestSentinelOutcome/disabled", "nonempty sentinel verification completed", true, false},
		{"sentinel empty", "TestSentinelOutcome/empty", "nonempty sentinel verification completed", true, false},
		{"replay", "TestApplyProposal_PerProposalIdempotency", "successful idempotency outcome replayed", true, true},
		{"conflict", "TestApplyProposal_PerProposalIdempotency", "idempotency body conflict rejected", true, true},
		{"frozen failure is not success", "TestIdempotencyCoverageExclusions/frozen_failure", "successful idempotency outcome replayed", true, false},
		{"expired replay", "TestIdempotencyCoverageExclusions/expired_matching", "successful idempotency outcome replayed", true, false},
		{"expired conflict", "TestIdempotencyCoverageExclusions/expired_different", "idempotency body conflict rejected", true, false},
		{"missing persisted volume", "TestVerifyPostCommitVolumes/missing", "committed volume is present in pebble", false, true},
		{"matching persisted volume", "TestVerifyPostCommitVolumes/matching", "committed volume is present in pebble", false, false},
		{"persisted volume IO error", "TestVerifyPostCommitVolumesReadFailure", "committed volume is present in pebble", false, false},
		{"missing update", "TestVerifyVolumeDeltasMatchPostingsRejectsCorruption/missing_update", "posting has a volume update", false, true},
		{"wrong delta", "TestVerifyVolumeDeltasMatchPostingsRejectsCorruption/wrong_amount", "volume delta matches posting quantities", false, true},
		{"unexpected delta", "TestVerifyVolumeDeltasMatchPostingsRejectsExtraBalancedPair", "nonzero volume delta is explained by postings", false, true},
		{"valid deltas present", "TestVerifyVolumeDeltasMatchPostingsValidScenarios", "posting has a volume update", false, false},
		{"valid deltas match", "TestVerifyVolumeDeltasMatchPostingsValidScenarios", "volume delta matches posting quantities", false, false},
		{"valid deltas explained", "TestVerifyVolumeDeltasMatchPostingsValidScenarios", "nonzero volume delta is explained by postings", false, false},
		{"all updates unbalanced", "TestWriteSetMergeRejectsUnbalancedVolumeUpdate", "all volume updates conserve double entry", false, true},
		{"persisted updates unbalanced", "TestWriteSetMergeRejectsUnbalancedPersistedVolumeUpdate", "persisted volume updates conserve double entry", false, true},
		{"logical updates balanced", "TestWriteSetMergeRejectsUnbalancedPersistedVolumeUpdate", "all volume updates conserve double entry", false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			found, err := antithesistest.Emitted(ctx, t.TempDir(), tc.run, tc.property, tc.condition)
			require.NoError(t, err)
			require.Equal(t, tc.wantHit, found, "property %q, condition %v", tc.property, tc.condition)
		})
	}
}

func TestIdempotencyCoverageExclusions(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"frozen_failure", "expired_matching", "expired_different"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			fsm, store, _ := newTestMachine(t)
			proposal := makeProposal(1, createLedgerOrder("idempotency-coverage"))
			proposal.Idempotency = &commonpb.Idempotency{Key: "key"}
			stored := &commonpb.IdempotencyKeyValue{
				Hash: fsm.processor.HashProposal(proposal), CreatedAt: 1,
				FirstLogSequence: 1, LogCount: 1,
			}
			if scenario == "frozen_failure" {
				stored.Failure = &commonpb.IdempotencyFailure{
					Reason: domain.ReasonCode(domain.ErrReasonTransactionNotFound), Message: "original failure",
				}
			} else {
				// Expiry is frozen on the outcome (EN-1827), so an expired one
				// carries an expires_at already reached: IdempotencyExpired is
				// nowMicros >= expiresAt, and the apply below stamps a later date.
				stored.ExpiresAt = IdempotencyExpiresAt(stored.GetCreatedAt(), 1)
				if scenario == "expired_different" {
					stored.Hash = []byte("different body")
				}
			}
			fsm.Registry.Idempotency.Put("key", stored)
			result, err := fsm.ApplyEntries(t.Context(), store, makeEntry(t, 1, proposal))
			require.NoError(t, err)
			if scenario == "frozen_failure" {
				var replayed *domain.ReplayedFailure
				require.ErrorAs(t, result.Results[0].Error, &replayed)
				require.True(t, result.Results[0].Replayed)
			} else {
				require.NoError(t, result.Results[0].Error)
				require.False(t, result.Results[0].Replayed)
				require.NotEmpty(t, result.Results[0].Logs)
			}
		})
	}
}

func TestPreparedBatchOutcomeFacts(t *testing.T) {
	t.Parallel()
	fsm, store, _ := newTestMachine(t)
	ctx := t.Context()
	result, err := fsm.ApplyEntries(ctx, store, makeEntry(t, 1, makeProposal(1, createLedgerOrder("outcomes"))))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)

	proposal := makeProposal(2,
		createTransactionOrder("outcomes", true, newPosting("world", "alice", "EUR", 20)),
		createTransactionOrder("outcomes", true, newPosting("world", "bob", "EUR", 10)))
	proposal.Idempotency = &commonpb.Idempotency{Key: "bulk"}
	first, err := fsm.PrepareEntries(ctx, store, makeEntry(t, 2, proposal))
	require.NoError(t, err)
	defer first.Close()
	require.Equal(t, 2, first.Result.Results[0].createdTransactions)
	created := first.Result.Results[0].Logs[0].GetCreatedLog().GetPayload().GetApply().GetLog().GetData().GetCreatedTransaction().GetTransaction()
	txID := created.GetId()

	// Prepare a different outcome before committing the first batch, just as
	// the pipelined applier can. Neither fact set may follow the live WriteSet.
	revertProposal := makeProposal(3, revertObservedTransactionOrder("outcomes", txID, created.GetPostings()))
	revertProposal.ExecutionPlan.Attributes = append(revertProposal.ExecutionPlan.Attributes,
		buildVolumePreloads([]*raftcmdpb.Order{proposal.GetOrders()[0]})...)
	second, err := fsm.PrepareEntries(ctx, store, makeEntry(t, 3, revertProposal))
	require.NoError(t, err)
	defer second.Close()
	require.NoError(t, second.Result.Results[0].Error)
	require.True(t, second.Result.Results[0].revertedTransaction)
	require.Zero(t, second.Result.Results[0].createdTransactions)
	require.Equal(t, 2, first.Result.Results[0].createdTransactions)
	require.False(t, first.Result.Results[0].revertedTransaction)
	require.NoError(t, fsm.CommitPreparedBatch(ctx, first))
	require.NoError(t, fsm.CommitPreparedBatch(ctx, second))

	// The same bulk now replays its original logs without counting as another
	// multi-transaction commit, even though its request still has two creates.
	proposal.Id = 4
	replay, err := fsm.PrepareEntries(ctx, store, makeEntry(t, 4, proposal))
	require.NoError(t, err)
	defer replay.Close()
	require.True(t, replay.Result.Results[0].Replayed)
	require.Zero(t, replay.Result.Results[0].createdTransactions)
	require.False(t, replay.Result.Results[0].revertedTransaction)
	require.NoError(t, fsm.CommitPreparedBatch(ctx, replay))

	failed, err := fsm.PrepareEntries(ctx, store, makeEntry(t, 5, makeProposal(5,
		createTransactionOrder("outcomes", true, newPosting("world", "charlie", "EUR", 3)),
		revertTransactionOrder("outcomes", 9999))))
	require.NoError(t, err)
	defer failed.Close()
	require.Error(t, failed.Result.Results[0].Error)
	require.Zero(t, failed.Result.Results[0].createdTransactions)
	require.False(t, failed.Result.Results[0].revertedTransaction)
	require.NoError(t, fsm.CommitPreparedBatch(ctx, failed))
}

func TestPreparedBatchWithoutCommit(t *testing.T) {
	t.Parallel()
	fsm, store, _ := newTestMachine(t)
	result, err := fsm.ApplyEntries(t.Context(), store, makeEntry(t, 1, makeProposal(1, createLedgerOrder("prepare"))))
	require.NoError(t, err)
	require.NoError(t, result.Results[0].Error)
	pb, err := fsm.PrepareEntries(t.Context(), store, makeEntry(t, 2, makeProposal(2,
		createTransactionOrder("prepare", true, newPosting("world", "alice", "EUR", 20)),
		createTransactionOrder("prepare", true, newPosting("world", "bob", "EUR", 10)))))
	require.NoError(t, err)
	defer pb.Close()
	require.Equal(t, 2, pb.Result.Results[0].createdTransactions)
}

func TestSentinelOutcome(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"enabled", "disabled", "empty"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			fsm, store, _ := newTestMachine(t)
			fsm.sentinelMode = name != "disabled"
			fsm.sentinel = dal.NewSentinelFactory(store, fsm.sentinelMode)
			proposal := makeProposal(1)
			if name != "empty" {
				proposal = makeProposal(1, createLedgerOrder("sentinel"),
					createTransactionOrder("sentinel", true, newPosting("world", "alice", "EUR", 20)))
			}
			result, err := fsm.ApplyEntries(t.Context(), store, makeEntry(t, 1, proposal))
			require.NoError(t, err)
			require.NoError(t, result.Results[0].Error)
		})
	}
}
