package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestNumscriptMetadataFailureSameAuditOnReplicas(t *testing.T) {
	t.Parallel()
	const ledger = "metadata-order"
	order := createTransactionOrder(ledger, false)
	order.GetLedgerScoped().GetApply().GetCreateTransaction().Script = &commonpb.Script{
		Plain: `
			vars { string $poison }
			set_tx_meta("a", $poison)
			set_tx_meta("b", $poison)
			send [USD/2 100] (source = @world destination = @users:alice)
		`,
		Vars: map[string]string{"poison": "safe\x00poison"},
	}
	proposal := makeProposal(2, order)
	proposal.Idempotency = &commonpb.Idempotency{Key: "metadata-order-key"}
	// The script's destination volume is discovered by admission. Declare its
	// zero preload explicitly here because the state fixture builds plans only
	// from literal posting orders.
	posting := newPosting("world", "users:alice", "USD/2", 100)
	proposal.ExecutionPlan.Attributes = append(proposal.ExecutionPlan.Attributes,
		buildVolumePreloads([]*raftcmdpb.Order{createTransactionOrder(ledger, false, posting)})...)
	entry := makeEntry(t, 2, proposal)

	var firstAudit []byte
	var firstHash []byte
	for range 20 {
		machine, store, attrs := newTestMachine(t)
		ctx := context.Background()
		created, err := machine.ApplyEntries(ctx, store, makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledger))))
		require.NoError(t, err)
		require.NoError(t, created.Results[0].Error)
		before := readBusinessProjections(t, store, attrs)

		result, err := machine.ApplyEntries(ctx, store, entry)
		require.NoError(t, err)
		var keyErr *domain.ErrMetadataKeyValidation
		require.ErrorAs(t, result.Results[0].Error, &keyErr)
		require.Equal(t, "a", keyErr.Key)
		require.Empty(t, result.Results[0].Logs)
		require.Equal(t, before, readBusinessProjections(t, store, attrs))
		audit := listAuditEntries(t, store, 0)
		require.Len(t, audit, 2)
		failure := audit[1].GetFailure()
		require.NotNil(t, failure)
		require.Equal(t, "a", failure.GetContext()["key"])
		auditBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(audit[1])
		require.NoError(t, err)
		if firstAudit == nil {
			firstAudit = auditBytes
			firstHash = append([]byte(nil), machine.State.LastAuditHash...)
		} else {
			require.Equal(t, firstAudit, auditBytes)
			require.Equal(t, firstHash, machine.State.LastAuditHash)
		}
		replay := proto.Clone(proposal).(*raftcmdpb.Proposal)
		replay.Id = 3
		replay.Date = &commonpb.Timestamp{Data: 1700000003}
		replayed, err := machine.ApplyEntries(ctx, store, makeEntry(t, 3, replay))
		require.NoError(t, err)
		require.True(t, replayed.Results[0].Replayed)
		require.EqualError(t, replayed.Results[0].Error, result.Results[0].Error.Error())
		require.Equal(t, firstHash, machine.State.LastAuditHash)
		require.Equal(t, before, readBusinessProjections(t, store, attrs))
	}
}
