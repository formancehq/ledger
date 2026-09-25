package processing

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// errReachedVolumes fails the first volume read so a test that expects the
// observation check to let an order through stops there instead of executing a
// whole revert. gomock's Finish is the real assertion: the expected volume read
// only happens if the check passed.
var errReachedVolumes = errors.New("reached the volume read")

// These tests pin the apply-time check on admission's revert-target
// observation. Admission reads the target from the local store with no read
// barrier, so a target that is committed but not yet applied there reads as
// absent and the order declares no volume coverage. Apply must reject before
// touching a volume, so a coverage miss keeps meaning "admission bug" rather
// than "stale view".
//
// The mock Scope is the oracle for "before any volume read": no volume
// expectation is registered, so any Volumes() call fails the test.

const staleTestLedger = "stale-ledger"

// revertObservationFixture wires the reads processRevertTransaction performs
// before the observation check: the reverted-bitset probe and the gated
// transaction state. Volumes are deliberately left unexpected.
//
// It serves every test in this file that reaches those reads, not only the
// stale-observation ones — the inconsistent-state and malformed-order cases go
// through the same setup.
func revertObservationFixture(t *testing.T, txID uint64, postings []*commonpb.Posting) (*MockScope, *raftcmdpb.LedgerBoundaries) {
	t.Helper()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	scope := NewMockScope(ctrl)
	txKey := domain.TransactionKey{LedgerName: staleTestLedger, ID: txID}

	scope.EXPECT().GetReverted(txKey).Return(false, nil)
	expectGetTransactionState(scope, txKey, (&commonpb.TransactionState{Postings: postings}).AsReader(), nil)

	return scope, &raftcmdpb.LedgerBoundaries{NextTransactionId: txID + 1, NextLogId: 1}
}

func revertTestPostings() []*commonpb.Posting {
	return []*commonpb.Posting{{
		Source:      "world",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(646),
		Asset:       "USD/2",
	}}
}

// TestProcessRevertTransaction_StaleObservationIsRetryable covers the observed
// Antithesis failure: the target was committed before this batch but not yet
// applied on the admitting node, so re-admitting against a fresher view
// converges.
func TestProcessRevertTransaction_StaleObservationIsRetryable(t *testing.T) {
	t.Parallel()

	const txID uint64 = 300

	scope, boundaries := revertObservationFixture(t, txID, revertTestPostings())

	payload, err := processRevertTransaction(
		staleTestLedger,
		&raftcmdpb.RevertTransactionOrder{TransactionId: txID},
		&Context{
			Scope:      scope,
			Boundaries: boundaries,
			LedgerInfo: (&commonpb.LedgerInfo{}).AsReader(),
			// Admission looked and saw nothing.
			RevertTargetDigest: domain.RevertTargetDigest(nil, false),
			// The target predates this batch, so a re-admission can see it.
			batchInitialNextTxID: map[string]uint64{staleTestLedger: txID + 1},
		},
	)

	require.Nil(t, payload)
	require.ErrorIs(t, err, domain.ErrStaleInputsResolution,
		"a stale admission view must be retryable so the client re-admits against fresh state")
	require.Equal(t, txID+1, boundaries.GetNextTransactionId(),
		"the rejection must not consume a transaction id")
}

// TestProcessRevertTransaction_TargetCreatedInBatchIsPermanent pins the case a
// retryable classification would turn into an infinite re-admit loop: the batch
// creates the target itself, so rejecting the batch un-creates it and every
// retry reproduces the identical observation.
func TestProcessRevertTransaction_TargetCreatedInBatchIsPermanent(t *testing.T) {
	t.Parallel()

	const txID uint64 = 300

	scope, boundaries := revertObservationFixture(t, txID, revertTestPostings())

	payload, err := processRevertTransaction(
		staleTestLedger,
		&raftcmdpb.RevertTransactionOrder{TransactionId: txID},
		&Context{
			Scope:              scope,
			Boundaries:         boundaries,
			LedgerInfo:         (&commonpb.LedgerInfo{}).AsReader(),
			RevertTargetDigest: domain.RevertTargetDigest(nil, false),
			// The ledger's horizon before this batch was the target's own id,
			// so the target is allocated by an earlier order in this batch.
			batchInitialNextTxID: map[string]uint64{staleTestLedger: txID},
		},
	)

	require.Nil(t, payload)

	var created *domain.ErrRevertTargetCreatedInBatch
	require.ErrorAs(t, err, &created)
	require.Equal(t, txID, created.TransactionID)
	require.NotErrorIs(t, err, domain.ErrStaleInputsResolution,
		"a target this batch creates can never be resolved by re-admission")
	require.Equal(t, domain.KindValidation, err.Kind(),
		"the client must see a permanent rejection, not a retryable one")
}

// TestProcessRevertTransaction_MatchingObservationProceeds pins that the check
// is inert when admission and apply agree.
func TestProcessRevertTransaction_MatchingObservationProceeds(t *testing.T) {
	t.Parallel()

	const txID uint64 = 300

	postings := revertTestPostings()
	scope, boundaries := revertObservationFixture(t, txID, postings)

	// Reaching the volume reads is the assertion: the check let the order
	// through. Fail the order there so the test stays focused on the check.
	expectGetVolume(scope, domain.NewVolumeKey(staleTestLedger, "users:001", "USD/2", ""), nil, errReachedVolumes)

	_, err := processRevertTransaction(
		staleTestLedger,
		&raftcmdpb.RevertTransactionOrder{TransactionId: txID},
		&Context{
			Scope:                scope,
			Boundaries:           boundaries,
			LedgerInfo:           (&commonpb.LedgerInfo{}).AsReader(),
			RevertTargetDigest:   domain.RevertTargetDigest(postings, true),
			batchInitialNextTxID: map[string]uint64{staleTestLedger: txID + 1},
		},
	)

	// Positive, not just "not one of the two mismatch reasons": without it any
	// other early rejection — including the check itself refusing a matching
	// digest — satisfies the negative assertions and the test proves nothing.
	require.ErrorIs(t, err, errReachedVolumes,
		"a matching observation must let the order through to the volume reads")

	require.NotErrorIs(t, err, domain.ErrStaleInputsResolution)

	var created *domain.ErrRevertTargetCreatedInBatch
	require.NotErrorAs(t, err, &created)
}

// TestProcessRevertTransaction_InvalidMetadataBeatsObservationCheck pins that a
// permanently invalid order is not reported as a stale observation.
//
// Both conditions hold here: the metadata exceeds the committed ceiling *and*
// admission's observation is stale. Answering STALE_INPUTS_RESOLUTION would
// advertise a retry that re-admission refuses identically — the same reason
// TRANSACTION_ALREADY_REVERTED outranks the observation check.
func TestProcessRevertTransaction_InvalidMetadataBeatsObservationCheck(t *testing.T) {
	t.Parallel()

	const txID uint64 = 300

	scope, boundaries := revertObservationFixture(t, txID, revertTestPostings())
	scope.EXPECT().GetClusterPolicy().Return(tightMetadataPolicy(4)).AnyTimes()

	payload, err := processRevertTransaction(
		staleTestLedger,
		&raftcmdpb.RevertTransactionOrder{
			TransactionId: txID,
			Metadata: map[string]*commonpb.MetadataValue{
				"k": commonpb.NewStringValue("far past the ceiling"),
			},
		},
		&Context{
			Scope:      scope,
			Boundaries: boundaries,
			LedgerInfo: (&commonpb.LedgerInfo{}).AsReader(),
			// Admission looked and saw nothing: the observation is stale too.
			RevertTargetDigest:   domain.RevertTargetDigest(nil, false),
			batchInitialNextTxID: map[string]uint64{staleTestLedger: txID + 1},
		},
	)

	require.Nil(t, payload)
	require.NotErrorIs(t, err, domain.ErrStaleInputsResolution,
		"a permanently invalid order must not be advertised as retryable")
	require.Equal(t, domain.KindValidation, err.Kind(),
		"the metadata rejection is the one the caller can act on")
}

// TestProcessRevertTransaction_NoDigestIsRejected pins that an order carrying no
// bound observation is refused rather than tolerated.
//
// Admission binds the digest on every revert order it emits, so an empty one is
// a malformed proposal. Treating it as "nothing to check" would be a fallback
// for an older v3 wire format — which the repository does not carry, v3 being
// unreleased — and would silently restore the very path this check closes.
func TestProcessRevertTransaction_NoDigestIsRejected(t *testing.T) {
	t.Parallel()

	const txID uint64 = 300

	scope, boundaries := revertObservationFixture(t, txID, revertTestPostings())

	payload, err := processRevertTransaction(
		staleTestLedger,
		&raftcmdpb.RevertTransactionOrder{TransactionId: txID},
		&Context{
			Scope:                scope,
			Boundaries:           boundaries,
			LedgerInfo:           (&commonpb.LedgerInfo{}).AsReader(),
			batchInitialNextTxID: map[string]uint64{staleTestLedger: txID + 1},
		},
	)

	require.Nil(t, payload)

	var invalid *domain.ErrInvalidExecutionPlan
	require.ErrorAs(t, err, &invalid,
		"a revert order with no bound observation is malformed, not exempt from the check")
	require.Contains(t, invalid.Reason_, "no target observation digest",
		"the missing-digest branch, not the sibling missing-horizon one")
}

// TestProcessRevertTransaction_NoBatchHorizonIsRejected pins that a mismatch
// with no recorded batch horizon is refused rather than defaulted.
//
// processApply records the horizon for every ledger before dispatch, so this is
// unreachable today. It is pinned because the silent fallback would be the worst
// of the two answers: a target the batch creates would be reported as merely
// stale, and the client would re-admit the identical batch forever.
func TestProcessRevertTransaction_NoBatchHorizonIsRejected(t *testing.T) {
	t.Parallel()

	const txID uint64 = 300

	scope, boundaries := revertObservationFixture(t, txID, revertTestPostings())

	payload, err := processRevertTransaction(
		staleTestLedger,
		&raftcmdpb.RevertTransactionOrder{TransactionId: txID},
		&Context{
			Scope:              scope,
			Boundaries:         boundaries,
			LedgerInfo:         (&commonpb.LedgerInfo{}).AsReader(),
			RevertTargetDigest: domain.RevertTargetDigest(nil, false),
			// Another ledger's horizon only: this one has none.
			batchInitialNextTxID: map[string]uint64{"other-ledger": txID + 1},
		},
	)

	require.Nil(t, payload)

	var invalid *domain.ErrInvalidExecutionPlan
	require.ErrorAs(t, err, &invalid)
	require.Contains(t, invalid.Reason_, "no recorded batch transaction-id horizon",
		"the missing-horizon branch, not the sibling missing-digest one")
	require.NotErrorIs(t, err, domain.ErrStaleInputsResolution,
		"an unclassifiable mismatch must not default to the retryable answer")
}

// TestProcessRevertTransaction_InconsistentStateNotSoftened pins the ordering
// that keeps invariant #7 intact: a transaction allocated with no postings is a
// broken projection and must keep surfacing as one, even though the bound
// observation also disagrees with it.
func TestProcessRevertTransaction_InconsistentStateNotSoftened(t *testing.T) {
	t.Parallel()

	const txID uint64 = 300

	scope, boundaries := revertObservationFixture(t, txID, nil)

	payload, err := processRevertTransaction(
		staleTestLedger,
		&raftcmdpb.RevertTransactionOrder{TransactionId: txID},
		&Context{
			Scope:                scope,
			Boundaries:           boundaries,
			LedgerInfo:           (&commonpb.LedgerInfo{}).AsReader(),
			RevertTargetDigest:   domain.RevertTargetDigest(revertTestPostings(), true),
			batchInitialNextTxID: map[string]uint64{staleTestLedger: txID + 1},
		},
	)

	require.Nil(t, payload)

	var inconsistent *domain.ErrTransactionStateInconsistent
	require.ErrorAs(t, err, &inconsistent,
		"an empty posting set is an invariant violation, never a retryable mismatch")
	require.NotErrorIs(t, err, domain.ErrStaleInputsResolution)
}

// TestProcessRevertTransaction_AlreadyRevertedBeatsObservationCheck pins that a
// definitive business outcome is not reclassified by the new check.
func TestProcessRevertTransaction_AlreadyRevertedBeatsObservationCheck(t *testing.T) {
	t.Parallel()

	const txID uint64 = 300

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	scope := NewMockScope(ctrl)
	scope.EXPECT().
		GetReverted(domain.TransactionKey{LedgerName: staleTestLedger, ID: txID}).
		Return(true, nil)

	payload, err := processRevertTransaction(
		staleTestLedger,
		&raftcmdpb.RevertTransactionOrder{TransactionId: txID},
		&Context{
			Scope:                scope,
			Boundaries:           &raftcmdpb.LedgerBoundaries{NextTransactionId: txID + 1},
			LedgerInfo:           (&commonpb.LedgerInfo{}).AsReader(),
			RevertTargetDigest:   domain.RevertTargetDigest(nil, false),
			batchInitialNextTxID: map[string]uint64{staleTestLedger: txID},
		},
	)

	require.Nil(t, payload)

	var reverted *domain.ErrTransactionAlreadyReverted
	require.ErrorAs(t, err, &reverted)
}

// TestProcessRevertTransaction_NotFoundBeatsObservationCheck pins that a revert
// of a transaction that never existed still audits as not-found.
func TestProcessRevertTransaction_NotFoundBeatsObservationCheck(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	t.Cleanup(ctrl.Finish)

	payload, err := processRevertTransaction(
		staleTestLedger,
		&raftcmdpb.RevertTransactionOrder{TransactionId: 999},
		&Context{
			Scope:                NewMockScope(ctrl),
			Boundaries:           &raftcmdpb.LedgerBoundaries{NextTransactionId: 5},
			LedgerInfo:           (&commonpb.LedgerInfo{}).AsReader(),
			RevertTargetDigest:   domain.RevertTargetDigest(nil, false),
			batchInitialNextTxID: map[string]uint64{staleTestLedger: 5},
		},
	)

	require.Nil(t, payload)

	var notFound *domain.ErrTransactionNotFound
	require.ErrorAs(t, err, &notFound)
}
