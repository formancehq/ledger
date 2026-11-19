package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// unplannableScript declares a monetary variable. Bound to a non-monetary
// value it fails planning before any balance or metadata read.
const unplannableScript = `vars { monetary $m }
send $m (
	source = @world
	destination = @alice
)`

// unplannableAfterReadScript reads @bank's balance while binding its variable,
// then reaches a scaling source, which planning rejects whatever the balance.
const unplannableAfterReadScript = `vars { monetary $b = balance(@bank, USD/2) }
send [USD/2 100] (
	source = @bank with scaling through @pool
	destination = @alice
)`

func skipReplayCreate(reference string, skippable bool, transaction *servicepb.CreateTransactionPayload) *servicepb.Request {
	transaction.Reference = reference
	apply := &servicepb.LedgerApplyRequest{
		Ledger: numscriptReplayLedger,
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: transaction}},
	}
	if skippable {
		apply.SkippableReasons = []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}
	}

	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: apply}}
}

func skipReplayPosting(source, destination string) *servicepb.CreateTransactionPayload {
	return &servicepb.CreateTransactionPayload{
		Postings: []*commonpb.Posting{{Source: source, Destination: destination, Asset: "USD/2", Amount: commonpb.NewUint256FromUint64(100)}},
	}
}

func skipReplayUnplannable() *servicepb.CreateTransactionPayload {
	return &servicepb.CreateTransactionPayload{
		Script: &commonpb.Script{Plain: unplannableScript, Vars: map[string]string{"m": "@world"}},
	}
}

func skipReplayUnplannableAfterRead() *servicepb.CreateTransactionPayload {
	return &servicepb.CreateTransactionPayload{Script: &commonpb.Script{Plain: unplannableAfterReadScript}}
}

func lastProposedOrder(t *testing.T, h *metadataReplayHarness) *raftcmdpb.Order {
	t.Helper()
	require.NotEmpty(t, h.proposals)
	orders := h.proposals[len(h.proposals)-1].GetBatch().GetOrders()
	require.NotEmpty(t, orders)

	return orders[len(orders)-1]
}

// nextPendingApply waits for the request under test to propose. A request that
// ends first was answered by admission alone.
func nextPendingApply[T any](t *testing.T, h *metadataReplayHarness, done <-chan T) func() {
	t.Helper()
	select {
	case apply := <-h.pendingApplies:
		return apply
	case got := <-done:
		require.FailNow(t, "the request ended without proposing", "%v", got)

		return nil
	}
}

// admissionRefusal returns what admission answers for the transaction when no
// FSM branch can skip it: the reference verdict a forwarded order must match.
func admissionRefusal(t *testing.T, transaction *servicepb.CreateTransactionPayload) domain.SerializableError {
	t.Helper()
	h := newMetadataReplayHarness(t)
	_, err := h.admission.Admit(attributedTestContext(context.Background()), numscriptReplayBatch("reference-verdict", skipReplayCreate("unused", false, transaction)))
	require.Empty(t, h.proposals, "without a skippable reference the refusal is admission's")
	var failure domain.SerializableError
	require.ErrorAs(t, err, &failure)

	return failure
}

// TestAdmitSkippedScriptReplaysAfterLedgerDelete reproduces the model-test
// sequence: a keyed bulk whose scripted create is skipped on its reference
// commits, the ledger is deleted (purging its references), and the same key
// must replay the retained outcome although the script cannot be planned.
func TestAdmitSkippedScriptReplaysAfterLedgerDelete(t *testing.T) {
	t.Parallel()
	for name, transaction := range map[string]func() *servicepb.CreateTransactionPayload{
		"before any read": skipReplayUnplannable,
		"after a read":    skipReplayUnplannableAfterRead,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			h := newMetadataReplayHarness(t)
			ctx := attributedTestContext(context.Background())
			_, err := h.admission.Admit(ctx, numscriptReplayBatch("", skipReplayCreate("r1", false, skipReplayPosting("world", "alice"))))
			require.NoError(t, err)

			request := numscriptReplayBatch("original", skipReplayCreate("r1", true, transaction()))
			original, err := h.admission.Admit(ctx, request)
			require.NoError(t, err)
			require.False(t, original.Replayed)
			require.Len(t, original.Logs, 1)
			require.True(t, lastProposedOrder(t, h).GetTechnical().GetNumscriptPlanningFailed(), "a predicted skip is still planned")

			_, err = h.admission.Admit(ctx, numscriptReplayBatch("", &servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: numscriptReplayLedger}}}))
			require.NoError(t, err)

			audit, sequence := h.machine.State.NextAuditSequenceID, h.machine.State.NextSequenceID
			replay, err := h.admission.Admit(ctx, request)
			require.NoError(t, err)
			require.True(t, replay.Replayed)
			require.Len(t, replay.Logs, 1)
			require.True(t, proto.Equal(original.Logs[0], replay.Logs[0]))
			require.NotNil(t, replay.Logs[0].GetPayload().GetApply().GetLog().GetData().GetOrderSkipped())
			require.Equal(t, audit, h.machine.State.NextAuditSequenceID)
			require.Equal(t, sequence, h.machine.State.NextSequenceID)

			request.GetUnsigned().IdempotencyKey = "fresh"
			_, err = h.admission.Admit(ctx, request)
			var failure domain.SerializableError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, domain.ErrReasonLedgerDeleted, failure.Reason())
		})
	}
}

// TestAdmitUnplannableSkippableScriptIsRefusedAtApply pins the fresh-request
// side: with the reference absent the FSM does not skip, re-plans the script
// and refuses with the failure admission gives an order it cannot skip. The
// refusal is audited and frozen under the key.
func TestAdmitUnplannableSkippableScriptIsRefusedAtApply(t *testing.T) {
	t.Parallel()
	for name, transaction := range map[string]func() *servicepb.CreateTransactionPayload{
		"before any read": skipReplayUnplannable,
		"after a read":    skipReplayUnplannableAfterRead,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			want := admissionRefusal(t, transaction())

			h := newMetadataReplayHarness(t)
			ctx := attributedTestContext(context.Background())
			request := numscriptReplayBatch("key", skipReplayCreate("fresh-reference", true, transaction()))
			audit, sequence := h.machine.State.NextAuditSequenceID, h.machine.State.NextSequenceID

			result, err := h.admission.Admit(ctx, request)
			require.Nil(t, result)
			var failure domain.SerializableError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, want.Reason(), failure.Reason())
			require.Equal(t, domain.FailureFactsOf(want), domain.FailureFactsOf(failure))
			require.Len(t, h.proposals, 1, "the refusal must be decided at apply")
			require.True(t, lastProposedOrder(t, h).GetTechnical().GetNumscriptPlanningFailed())
			require.Equal(t, audit+1, h.machine.State.NextAuditSequenceID)
			require.Equal(t, sequence, h.machine.State.NextSequenceID)

			_, err = h.admission.Admit(ctx, request)
			var replay *domain.ReplayedFailure
			require.ErrorAs(t, err, &replay)
			require.Equal(t, want.Reason(), replay.Reason())
			require.Equal(t, audit+1, h.machine.State.NextAuditSequenceID)
		})
	}
}

// TestAdmitUnplannableSkippableScriptSkipsOnConcurrentReference covers a
// reference created between admission and apply: admission read it absent and
// forwarded the planning failure, and the FSM skips the order.
func TestAdmitUnplannableSkippableScriptSkipsOnConcurrentReference(t *testing.T) {
	t.Parallel()
	h := newMetadataReplayHarness(t)
	ctx := attributedTestContext(context.Background())
	h.pendingApplies = make(chan func(), 2)
	createDone := make(chan error, 1)
	go func() {
		_, err := h.admission.Admit(ctx, numscriptReplayBatch("", skipReplayCreate("r1", false, skipReplayPosting("world", "alice"))))
		createDone <- err
	}()
	applyCreate := <-h.pendingApplies

	type outcome struct {
		logs int
		err  error
	}
	scriptDone := make(chan outcome, 1)
	go func() {
		result, err := h.admission.Admit(ctx, numscriptReplayBatch("key", skipReplayCreate("r1", true, skipReplayUnplannable())))
		if err != nil {
			scriptDone <- outcome{err: err}

			return
		}
		scriptDone <- outcome{logs: len(result.Logs)}
	}()
	applyScript := nextPendingApply(t, h, scriptDone)
	require.True(t, lastProposedOrder(t, h).GetTechnical().GetNumscriptPlanningFailed())

	applyCreate()
	require.NoError(t, <-createDone)
	applyScript()
	got := <-scriptDone
	require.NoError(t, got.err, "the reference exists at apply, so the order is skipped")
	require.Equal(t, 1, got.logs)
}

// TestAdmitUnplannableSkippableScriptIsStaleWhenInputChanges covers an input
// that changes between admission and apply: the failure admission observed is
// no longer proven, so the order is rejected as retryable, unaudited, and the
// retry reaches the definitive refusal.
func TestAdmitUnplannableSkippableScriptIsStaleWhenInputChanges(t *testing.T) {
	t.Parallel()
	h := newMetadataReplayHarness(t)
	ctx := attributedTestContext(context.Background())
	h.pendingApplies = make(chan func(), 2)
	fundDone := make(chan error, 1)
	go func() {
		_, err := h.admission.Admit(ctx, numscriptReplayBatch("", skipReplayCreate("", false, skipReplayPosting("world", "bank"))))
		fundDone <- err
	}()
	applyFund := <-h.pendingApplies

	request := numscriptReplayBatch("key", skipReplayCreate("fresh-reference", true, skipReplayUnplannableAfterRead()))
	scriptDone := make(chan error, 1)
	go func() {
		_, err := h.admission.Admit(ctx, request)
		scriptDone <- err
	}()
	applyScript := nextPendingApply(t, h, scriptDone)

	applyFund()
	require.NoError(t, <-fundDone)
	audit := h.machine.State.NextAuditSequenceID
	applyScript()
	require.ErrorIs(t, <-scriptDone, domain.ErrStaleInputsResolution)
	require.Equal(t, audit, h.machine.State.NextAuditSequenceID, "the stale rejection is not audited")

	h.pendingApplies = nil
	_, err := h.admission.Admit(ctx, request)
	var failure domain.SerializableError
	require.ErrorAs(t, err, &failure)
	require.Equal(t, domain.ErrNumscriptScalingUnsupported.Reason(), failure.Reason())
	require.Equal(t, audit+1, h.machine.State.NextAuditSequenceID)
}

// TestAdmitSkippableScriptWithoutKey pins the unkeyed behavior: a predicted
// skip is forwarded and skipped whatever its script, while an order admission
// does not predict to skip keeps the fail-fast refusal.
func TestAdmitSkippableScriptWithoutKey(t *testing.T) {
	t.Parallel()
	h := newMetadataReplayHarness(t)
	ctx := attributedTestContext(context.Background())
	_, err := h.admission.Admit(ctx, numscriptReplayBatch("", skipReplayCreate("r1", false, skipReplayPosting("world", "alice"))))
	require.NoError(t, err)

	skipped, err := h.admission.Admit(ctx, numscriptReplayBatch("", skipReplayCreate("r1", true, skipReplayUnplannable())))
	require.NoError(t, err)
	require.Len(t, skipped.Logs, 1)

	proposed := len(h.proposals)
	_, err = h.admission.Admit(ctx, numscriptReplayBatch("", skipReplayCreate("fresh-reference", true, skipReplayUnplannable())))
	require.Error(t, err)
	require.Len(t, h.proposals, proposed, "no key and no predicted skip: refused before proposing")
}

// TestAdmitPlannedSkippedScriptLeavesNoTrace pins skip parity for a predicted
// skip whose script plans: it is forwarded with its coverage, the FSM skips
// it, and a later order of the batch resolves as if it never ran.
func TestAdmitPlannedSkippedScriptLeavesNoTrace(t *testing.T) {
	t.Parallel()
	h := newMetadataReplayHarness(t)
	ctx := attributedTestContext(context.Background())
	_, err := h.admission.Admit(ctx, numscriptReplayBatch("", skipReplayCreate("r1", false, skipReplayPosting("world", "bank"))))
	require.NoError(t, err)

	drain := &servicepb.CreateTransactionPayload{Script: &commonpb.Script{Plain: `send [USD/2 100] (
	source = @bank
	destination = @alice
)`}}
	spendAll := &servicepb.CreateTransactionPayload{Script: &commonpb.Script{Plain: `vars { monetary $b = balance(@bank, USD/2) }
send $b (
	source = @bank
	destination = @carol
)`}}
	result, err := h.admission.Admit(ctx, numscriptReplayBatch("key",
		skipReplayCreate("r1", true, drain),
		skipReplayCreate("", false, spendAll),
	))
	require.NoError(t, err, "the second order's inputs hash must not embed the skipped order's effects")
	require.Len(t, result.Logs, 2)

	orders := h.proposals[len(h.proposals)-1].GetBatch().GetOrders()
	require.False(t, orders[0].GetTechnical().GetNumscriptPlanningFailed())
	require.NotEmpty(t, orders[0].GetTechnical().GetCoverageBits())
}
