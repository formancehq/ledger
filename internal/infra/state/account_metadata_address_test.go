package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestAccountMetadataAddressFailureSameAuditOnReplicas(t *testing.T) {
	t.Parallel()
	for _, keyed := range []bool{false, true} {
		name := "unkeyed"
		if keyed {
			name = "fresh-key"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			const ledger = "metadata-address"
			order := createTransactionOrder(ledger, false, newPosting("world", "users:alice", "USD", 100))
			metadata := map[string]*commonpb.MetadataMap{
				"":            {Values: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")}},
				"bad!":        {Values: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")}},
				"users:alice": {Values: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")}},
			}
			order.GetLedgerScoped().GetApply().GetCreateTransaction().AccountMetadata = metadata
			proposal := makeProposal(2, order)
			if keyed {
				proposal.Idempotency = &commonpb.Idempotency{Key: "fresh-address-key"}
			}
			// Every replica independently decodes these exact committed bytes at
			// the same index, rather than sharing an already-decoded Go map.
			entry := makeEntry(t, 2, proposal)
			setup := makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledger)))
			var firstAudit, firstHash []byte
			for range 2 {
				machine, store, attrs := newTestMachine(t)
				created, err := machine.ApplyEntries(t.Context(), store, setup)
				require.NoError(t, err)
				require.NoError(t, created.Results[0].Error)
				before := readBusinessProjections(t, store, attrs)
				reader, err := store.NewReadHandle()
				require.NoError(t, err)
				beforeMetadata, err := attrs.Metadata.ComputeAllForPrefix(reader, nil)
				require.NoError(t, reader.Close())
				require.NoError(t, err)

				applied, err := machine.ApplyEntries(t.Context(), store, entry)
				require.NoError(t, err)
				result := applied.Results[0]
				require.ErrorIs(t, result.Error, domain.ErrAccountAddressEmpty)
				require.False(t, result.Replayed)
				require.True(t, result.AuditEntryWritten)
				require.Empty(t, result.Logs)
				require.Equal(t, before, readBusinessProjections(t, store, attrs))
				reader, err = store.NewReadHandle()
				require.NoError(t, err)
				afterMetadata, err := attrs.Metadata.ComputeAllForPrefix(reader, nil)
				require.NoError(t, reader.Close())
				require.NoError(t, err)
				require.Equal(t, beforeMetadata, afterMetadata)
				audits := listAuditEntries(t, store, 0)
				require.Len(t, audits, 2)
				failure := audits[1].GetFailure()
				require.NotNil(t, failure)
				require.True(t, proto.Equal(domain.ErrAccountAddressEmpty.FailureDetails(), failure.GetDetails()))
				auditBytes, err := proto.MarshalOptions{Deterministic: true}.Marshal(audits[1])
				require.NoError(t, err)
				if firstAudit == nil {
					firstAudit, firstHash = auditBytes, append([]byte(nil), machine.State.LastAuditHash...)
				} else {
					require.Equal(t, firstAudit, auditBytes)
					require.Equal(t, firstHash, machine.State.LastAuditHash)
				}
				_, frozen := machine.Registry.Idempotency.Get("fresh-address-key")
				require.Equal(t, keyed, frozen)
				if keyed {
					replay := proto.Clone(proposal).(*raftcmdpb.Proposal)
					replay.Id = 3
					replay.Date = &commonpb.Timestamp{Data: 1700000003}
					replayed, err := machine.ApplyEntries(t.Context(), store, makeEntry(t, 3, replay))
					require.NoError(t, err)
					require.True(t, replayed.Results[0].Replayed)
					var frozenError domain.SerializableError
					require.ErrorAs(t, replayed.Results[0].Error, &frozenError)
					require.True(t, proto.Equal(domain.ErrAccountAddressEmpty.FailureDetails(), frozenError.FailureDetails()))
					require.False(t, replayed.Results[0].AuditEntryWritten)
					require.Equal(t, firstHash, machine.State.LastAuditHash)
					require.Equal(t, before, readBusinessProjections(t, store, attrs))
				}
			}
		})
	}
}
