package state

// This probe deliberately uses only the RC.1 API. scripts/check-proposal-payload
// compiles this same source against the released tree and the candidate. It
// compares real FSM/audit results from separate binaries, not two modes of a
// single decoder. Numeric wire tags below are the compatibility test oracle.

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestProposalPayloadCompatibilityProbe(t *testing.T) {
	output := os.Getenv("LEDGER_PROPOSAL_PROBE_OUTPUT")
	if output == "" {
		t.Skip("run scripts/check-proposal-payload for the two-binary compatibility test")
	}
	machine, store, _ := newTestMachine(t)
	policy := machine.State.ClusterPolicy.CloneVT()
	policy.Revision = 2
	policy.IdempotencyTtlMicros = 9000
	orders := []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{
			SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{Policy: policy},
		}}}},
		createLedgerOrder("compatibility"),
		createLedgerOrder("compatibility"), // Audit a business rejection too.
	}
	var report struct {
		Policy    *commonpb.ClusterPolicy
		Audit     []*auditpb.AuditEntry
		Items     [][]*auditpb.AuditItem
		Applied   uint64
		Timestamp uint64
	}
	for i, order := range orders {
		index := uint64(i + 1)
		p := sealProposal(makeProposal(index, order))
		data, err := p.MarshalVT()
		require.NoError(t, err)
		if os.Getenv("LEDGER_PROPOSAL_PROBE_ENCODING") == "payload" {
			var header, batch []byte
			for rest := data; len(rest) > 0; {
				number, typ, tagLen := protowire.ConsumeTag(rest)
				require.Positive(t, tagLen)
				valueLen := protowire.ConsumeFieldValue(number, typ, rest[tagLen:])
				require.GreaterOrEqual(t, valueLen, 0)
				field := rest[:tagLen+valueLen]
				if number == 2 {
					batch = protowire.AppendTag(batch, 1, protowire.BytesType)
					batch = append(batch, field[tagLen:]...)
				} else {
					header = append(header, field...)
				}
				rest = rest[len(field):]
			}
			if policyOrder := order.GetSystemScoped().GetSetClusterPolicy(); policyOrder != nil {
				control, err := policyOrder.MarshalVT()
				require.NoError(t, err)
				header = protowire.AppendTag(header, 12, protowire.BytesType)
				data = protowire.AppendBytes(header, control)
			} else {
				header = protowire.AppendTag(header, 11, protowire.BytesType)
				data = protowire.AppendBytes(header, batch)
			}
		}
		result, err := machine.ApplyEntries(t.Context(), store, &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data})
		require.NoError(t, err)
		require.True(t, result.Results[0].AuditEntryWritten)
		reader, err := store.NewDirectReadHandle()
		require.NoError(t, err)
		audit, err := query.ReadLastAuditEntry(reader)
		require.NoError(t, err)
		items, err := query.ReadAuditItems(t.Context(), reader, audit.GetSequence())
		require.NoError(t, err)
		report.Audit = append(report.Audit, audit)
		report.Items = append(report.Items, items)
		require.NoError(t, reader.Close())
	}
	report.Policy = machine.State.ClusterPolicy
	report.Applied = machine.State.LastAppliedIndex
	report.Timestamp = machine.State.LastAppliedTimestamp
	data, err := json.Marshal(report)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(output, data, 0o600))
}
