package processing

import (
	"bytes"
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func processRevertTransaction(ledger string, order *raftcmdpb.RevertTransactionOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	boundaries := ctx.Boundaries
	s := ctx.Scope
	info := ctx.LedgerInfo

	txKey := domain.TransactionKey{
		LedgerName: ledger,
		ID:         order.GetTransactionId(),
	}

	// Check if transaction exists (ID must be less than next transaction ID)
	if order.GetTransactionId() >= boundaries.GetNextTransactionId() {
		return nil, &domain.ErrTransactionNotFound{TransactionID: order.GetTransactionId()}
	}

	// Check if the transaction is already reverted (bitset lookup, never errors)
	reverted, err := s.GetReverted(txKey)
	if err != nil {
		return nil, domain.StoreFailure("checking reverted status", err)
	}

	if reverted {
		return nil, &domain.ErrTransactionAlreadyReverted{TransactionID: order.GetTransactionId()}
	}

	// The original postings live on the transaction's own state, which
	// admission preloaded (addTransactionTargetNeeds) and the FSM reads
	// through the coverage gate — never off the order, which carries only
	// caller intent (invariant #8). A genuinely non-existent tx is already
	// rejected by the boundary check above; a miss here means an allocated
	// tx with no state (IDs have no gaps), i.e. a cache/Pebble desync — surface loudly
	// with the invariant-violation error class (invariant #7).
	origStateReader, err := s.TransactionStates().Get(txKey)
	if errors.Is(err, domain.ErrNotFound) {
		return nil, &domain.ErrTransactionStateInconsistent{TransactionID: order.GetTransactionId(), Operation: "revert"}
	}

	if err != nil {
		return nil, domain.StoreFailure("getting original transaction state", err)
	}

	origState := origStateReader.Mutate()

	originalPostings := origState.GetPostings()
	if len(originalPostings) == 0 {
		// Create rejects empty transactions, so a stored state always carries
		// at least one posting; an empty set here is an inconsistent projection
		// (invariant #7), not a revertable transaction.
		return nil, &domain.ErrTransactionStateInconsistent{TransactionID: order.GetTransactionId(), Operation: "revert"}
	}

	// Caller metadata is validated before the observation check, for the same
	// reason TRANSACTION_ALREADY_REVERTED outranks it: a permanently invalid
	// order is invalid however fresh the view is, so classifying it as a stale
	// observation would advertise a retry that re-admission refuses identically.
	// It reads only the committed cluster policy, no coverage-gated key, so
	// running it first cannot reach the gate ahead of the check below.
	if err := validateMetadataAtApply(order.GetMetadata(), ctx); err != nil {
		return nil, err
	}

	// Admission declared this order's volume coverage from its own read of the
	// target, taken from the local store with no read barrier. If what it saw
	// differs from what apply just read through the gate, the declared coverage
	// does not describe the volumes below and the order must not proceed —
	// reaching applyPosting would trip the coverage gate, whose contract is that
	// a miss means an admission bug rather than a stale view.
	//
	// This runs after the invariant checks above on purpose: an allocated
	// transaction with no state, or with no postings, is a broken projection and
	// must keep surfacing as such rather than being softened into a retryable
	// mismatch.
	if err := checkRevertTargetObservation(ledger, order.GetTransactionId(), originalPostings, ctx); err != nil {
		return nil, err
	}

	// Create reversed postings and update volumes
	// For a revert: original destination becomes source, original source becomes destination.
	// Color carries over from the original posting — the funds were segregated under
	// (account, asset, color) on the way out, so they must return under the same bucket.
	revertPostings := make([]*commonpb.Posting, len(originalPostings))
	for i, originalPosting := range originalPostings {
		revertPostings[i] = &commonpb.Posting{
			Source:      originalPosting.GetDestination(),
			Destination: originalPosting.GetSource(),
			Amount:      originalPosting.GetAmount(),
			Asset:       originalPosting.GetAsset(),
			Color:       originalPosting.GetColor(),
		}
	}

	// Validate reversed postings against account types.
	if compiled := compiledTypesFor(ctx.CompiledTypes, ledger, info); len(compiled) > 0 {
		if typeErr := validatePostingsAgainstAccountTypes(revertPostings, compiled, info.GetDefaultEnforcementMode()); typeErr != nil {
			return nil, typeErr
		}
	}

	// Reject exhaustion before applying the compensating postings or marking
	// the original transaction reverted. See processCreateTransaction.
	revertTxID := boundaries.GetNextTransactionId()
	advancedTransactionID, exhausted := domain.CheckedNextSequence(revertTxID, domain.SequenceCounterTransactionID)
	if exhausted != nil {
		return nil, exhausted
	}

	for _, posting := range revertPostings {
		// Apply the reversed posting (skip balance check if force is set)
		err := applyPosting(s, ledger, posting, order.GetForce(), ctx.AssetCache)
		if err != nil {
			return nil, err
		}
	}

	// Mark the original transaction as reverted
	s.PutReverted(txKey, true)

	// Consume the preflighted transaction ID for the compensating transaction.
	boundaries.NextTransactionId = advancedTransactionID

	// posting_count and revert_count are no longer maintained on
	// LedgerBoundaries — the usagebuilder derives them from the audit
	// chain. See EN-1420.

	// Resolve the revert timestamp. When at_effective_date is set, the compensating
	// transaction inherits the original's effective timestamp (parity with
	// formancehq/ledger). Otherwise it stamps with the current FSM date.
	// origState.Timestamp is populated at create time on every code path; missing
	// it on an at_effective_date revert means we observed an inconsistent state.
	revertTimestamp := s.GetDate().Mutate()
	if order.GetAtEffectiveDate() {
		if origState.GetTimestamp() == nil {
			return nil, &domain.ErrTransactionStateInconsistent{TransactionID: order.GetTransactionId(), Operation: "revert at_effective_date"}
		}

		revertTimestamp = origState.GetTimestamp()
	}

	// Record the reversion on the original transaction's state: the id of the
	// compensating transaction and the effective time it was reverted.
	origState.RevertedByTransaction = revertTxID
	origState.RevertedAt = revertTimestamp
	s.TransactionStates().Put(txKey, origState)

	// Store the revert transaction's state (include metadata from the revert
	// order); RevertsTransaction back-links it to the transaction it compensates.
	s.TransactionStates().Put(domain.TransactionKey{LedgerName: ledger, ID: revertTxID}, &commonpb.TransactionState{
		CreatedByLog:       s.GetNextSequenceID(),
		Metadata:           order.GetMetadata(),
		Timestamp:          revertTimestamp,
		Postings:           revertPostings,
		RevertsTransaction: order.GetTransactionId(),
	})

	// Post-commit volumes are part of every persisted transaction: compute
	// them unconditionally from the volume state after the compensating
	// postings applied (before any proposal-level ephemeral purge). The
	// compensating transaction carries its own post-revert snapshot; the
	// original keeps its creation-time snapshot untouched.
	postCommitVolumes, pcvErr := buildPostCommitVolumes(s, ledger, revertPostings)
	if pcvErr != nil {
		return nil, pcvErr
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: order.GetTransactionId(),
				RevertTransaction: &commonpb.Transaction{
					Postings:           revertPostings,
					Metadata:           order.GetMetadata(),
					Timestamp:          revertTimestamp,
					Id:                 revertTxID,
					InsertedAt:         s.GetDate().Mutate(),
					UpdatedAt:          s.GetDate().Mutate(),
					RevertsTransaction: order.GetTransactionId(),
					PostCommitVolumes:  postCommitVolumes,
				},
			},
		},
	}, nil
}

// checkRevertTargetObservation compares the transaction state apply just read
// through the coverage gate with what admission observed when it declared this
// order's volume coverage.
//
// Admission reads the target from the local store with no read barrier
// (Admission.observeRevertTarget), so a target that is committed but not yet
// applied on that node reads as absent and the order declares no volume keys.
// Apply then finds the real postings and would read volumes the plan never
// declared. Rejecting here keeps the coverage gate meaning what it documents: a
// miss is an admission bug, not a stale view.
//
// Reaching this function already means the target passed the handler's checks
// on it, and those keep precedence: an id beyond the ledger boundary is
// ErrTransactionNotFound, an already-reverted target is
// ErrTransactionAlreadyReverted, and an allocated target with no state or no
// postings is ErrTransactionStateInconsistent — a stale observation of any of
// them answers with that reason, not with a mismatch classification.
//
// The two mismatch causes need different answers, and the difference is whether
// a re-admission could ever see what apply sees:
//
//   - the target existed before this batch → the observation was merely stale,
//     so reject with the retryable ErrStaleInputsResolution and let the client
//     re-admit against a view that now includes it;
//   - the target is created by this batch → the whole batch is rejected, so the
//     create never lands and re-admitting the identical batch reproduces the
//     same observation forever. Reject permanently instead.
//
// A revert order with no digest is not tolerated. Admission is the only producer
// of one (mirror reverts have their own handler), and it binds the observation
// unconditionally, so an empty digest is a malformed proposal rather than an
// older wire format — v3 is unreleased and carries no compatibility fallbacks.
// Accepting it would silently disable the check and let the stale-target path
// reach applyPosting again. A missing batch transaction-id horizon is refused
// the same way, for the same reason: without it the two causes cannot be told
// apart, and defaulting to the retryable one would re-create the re-admit loop
// this classification exists to prevent.
func checkRevertTargetObservation(
	ledger string,
	transactionID uint64,
	originalPostings []*commonpb.Posting,
	ctx *Context,
) domain.Describable {
	expected := ctx.RevertTargetDigest
	if len(expected) == 0 {
		return &domain.ErrInvalidExecutionPlan{
			Reason_: "revert order carries no target observation digest",
		}
	}

	if bytes.Equal(expected, domain.RevertTargetDigest(originalPostings, true)) {
		return nil
	}

	initial, ok := ctx.batchInitialNextTxID[ledger]
	if !ok {
		// processApply records the horizon for every ledger it touches before
		// dispatching, so a revert cannot reach here without one. Falling
		// through to the retryable answer would be the worst available failure:
		// a target this batch creates would be classified as merely stale, and
		// the client would re-admit an identical batch forever (invariant #7).
		return &domain.ErrInvalidExecutionPlan{
			Reason_: "revert apply reached the observation check with no recorded batch transaction-id horizon",
		}
	}

	if transactionID >= initial {
		return &domain.ErrRevertTargetCreatedInBatch{TransactionID: transactionID}
	}

	return domain.ErrStaleInputsResolution
}
