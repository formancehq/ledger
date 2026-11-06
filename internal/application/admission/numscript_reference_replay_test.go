package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

const numscriptReplayLedger = "metadata-replay"

func numscriptReplayBatch(key string, requests ...*servicepb.Request) *servicepb.ApplyRequest {
	return &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_Unsigned{Unsigned: &servicepb.ApplyBatch{IdempotencyKey: key, Requests: requests}}}
}

func numscriptReplayReference(name, version string) *servicepb.Request {
	transaction := &servicepb.CreateTransactionPayload{ScriptReference: &servicepb.ScriptReference{Name: name, Version: version}}
	action := &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: transaction}}

	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: numscriptReplayLedger, Action: action}}}
}

func numscriptReplaySave(name, version string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_SaveNumscript{SaveNumscript: &servicepb.SaveNumscriptRequest{
		Ledger: numscriptReplayLedger, Name: name, Version: version,
		Content: `send [USD/2 100] (source = @world destination = @alice)`,
	}}}
}

// requireNumscriptNotFoundAudit checks that the last audit entry is the failure
// the client received and that it records the attempted reference order.
func (h *metadataReplayHarness) requireNumscriptNotFoundAudit(t *testing.T, err error) {
	t.Helper()
	ctx := context.Background()
	var failure domain.SerializableError
	require.ErrorAs(t, err, &failure)
	reader, readErr := h.admission.store.NewReadHandle()
	require.NoError(t, readErr)
	defer func() { require.NoError(t, reader.Close()) }()
	entry, readErr := query.ReadLastAuditEntry(reader)
	require.NoError(t, readErr)
	require.NotNil(t, entry.GetFailure())
	facts, readErr := domain.FailureFactsFromDetails(entry.GetFailure().GetDetails())
	require.NoError(t, readErr)
	require.Equal(t, domain.FailureFactsOf(failure), facts)
	items, readErr := query.ReadAuditItems(ctx, reader, entry.GetSequence())
	require.NoError(t, readErr)
	require.Len(t, items, 1)
	order := &raftcmdpb.Order{}
	require.NoError(t, order.UnmarshalVT(items[0].GetSerializedOrder()))
	require.NotNil(t, createTxOf(order).GetNumscriptReference())
	require.Empty(t, createTxOf(order).GetScript().GetPlain())
}

// TestAdmitMissingNumscriptReferenceIsAuditedAtApply pins that a reference
// admission cannot resolve reaches the FSM, which records the definitive
// refusal with the same reason and context the client receives. A keyed
// refusal stays frozen after the script is saved.
func TestAdmitMissingNumscriptReferenceIsAuditedAtApply(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"1.0.0", "latest"} {
		for _, key := range []string{"", "new-key"} {
			t.Run(version+"/key="+key, func(t *testing.T) {
				t.Parallel()
				h := newMetadataReplayHarness(t)
				ctx := attributedTestContext(context.Background())
				request := numscriptReplayBatch(key, numscriptReplayReference("missing", version))
				audit, sequence := h.machine.State.NextAuditSequenceID, h.machine.State.NextSequenceID

				result, err := h.admission.Admit(ctx, request)
				require.Nil(t, result)
				var notFound *domain.ErrNumscriptNotFound
				require.ErrorAs(t, err, &notFound)
				want := domain.ErrNumscriptNotFound{Name: "missing"}
				if version != "latest" {
					want.Version = version
				}
				require.Equal(t, want, *notFound)
				require.Len(t, h.proposals, 1, "the refusal must be decided at apply")
				require.Equal(t, audit+1, h.machine.State.NextAuditSequenceID)
				require.Equal(t, sequence, h.machine.State.NextSequenceID)
				h.requireNumscriptNotFoundAudit(t, err)

				if key == "" {
					return
				}

				_, err = h.admission.Admit(ctx, numscriptReplayBatch("", numscriptReplaySave("missing", "1.0.0")))
				require.NoError(t, err)
				audit, sequence = h.machine.State.NextAuditSequenceID, h.machine.State.NextSequenceID
				_, err = h.admission.Admit(ctx, request)
				var replay *domain.ReplayedFailure
				require.ErrorAs(t, err, &replay)
				require.Equal(t, domain.ErrReasonNumscriptNotFound, replay.Reason())
				require.Equal(t, audit, h.machine.State.NextAuditSequenceID)
				require.Equal(t, sequence, h.machine.State.NextSequenceID)
			})
		}
	}
}

// TestAdmitNumscriptReferenceReplaysAfterLedgerDelete reproduces the model-test
// sequence: a keyed bulk referencing a library script commits, the ledger is
// deleted (purging its library), and the same key must replay the retained
// outcome. A fresh key is a new attempt and its refusal is audited.
func TestAdmitNumscriptReferenceReplaysAfterLedgerDelete(t *testing.T) {
	t.Parallel()
	for _, version := range []string{"1.0.0", "latest"} {
		t.Run(version, func(t *testing.T) {
			t.Parallel()
			h := newMetadataReplayHarness(t)
			ctx := attributedTestContext(context.Background())
			_, err := h.admission.Admit(ctx, numscriptReplayBatch("", numscriptReplaySave("ns", "1.0.0")))
			require.NoError(t, err)

			request := numscriptReplayBatch("original", numscriptReplayReference("ns", version), numscriptReplayReference("ns", version))
			original, err := h.admission.Admit(ctx, request)
			require.NoError(t, err)
			require.False(t, original.Replayed)
			require.Len(t, original.Logs, 2)

			_, err = h.admission.Admit(ctx, numscriptReplayBatch("", &servicepb.Request{Type: &servicepb.Request_DeleteLedger{DeleteLedger: &servicepb.DeleteLedgerRequest{Name: numscriptReplayLedger}}}))
			require.NoError(t, err)

			audit, sequence := h.machine.State.NextAuditSequenceID, h.machine.State.NextSequenceID
			replay, err := h.admission.Admit(ctx, request)
			require.NoError(t, err)
			require.True(t, replay.Replayed)
			require.Equal(t, original.Logs, replay.Logs)
			require.Equal(t, audit, h.machine.State.NextAuditSequenceID)
			require.Equal(t, sequence, h.machine.State.NextSequenceID)

			request.GetUnsigned().IdempotencyKey = "fresh"
			_, err = h.admission.Admit(ctx, request)
			var failure domain.SerializableError
			require.ErrorAs(t, err, &failure)
			require.Equal(t, domain.ErrReasonLedgerDeleted, failure.Reason(), "the deleted ledger is refused before its library is read")
			require.Equal(t, audit+1, h.machine.State.NextAuditSequenceID)
			require.Equal(t, sequence, h.machine.State.NextSequenceID)
		})
	}
}
