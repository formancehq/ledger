package state

// This probe deliberately uses only the RC.1 API. scripts/check-proposal-payload
// compiles this same source against the released tree and the candidate. It
// compares real FSM/audit results from separate binaries, not two modes of a
// single decoder. Numeric wire tags below are the compatibility test oracle.

import (
	"encoding/json"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/protobuf/encoding/protowire"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/processing"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

func TestProposalPayloadCompatibilityProbe(t *testing.T) {
	output := os.Getenv("LEDGER_PROPOSAL_PROBE_OUTPUT")
	if output == "" {
		t.Skip("run scripts/check-proposal-payload for the two-binary compatibility test")
	}
	dir := os.Getenv("LEDGER_PROPOSAL_PROBE_DIR")
	if dir == "" {
		dir = t.TempDir()
	}
	phase := os.Getenv("LEDGER_PROPOSAL_PROBE_PHASE")
	logger := logging.Testing()
	meter := noop.NewMeterProvider().Meter("test")
	store, err := dal.NewStore(filepath.Join(dir, "data"), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	defer func() { require.NoError(t, store.Close()) }()
	log, err := wal.New(filepath.Join(dir, "wal"), logger, meter)
	require.NoError(t, err)
	defer func() { require.NoError(t, log.Close()) }()
	if phase != "resume" && phase != "verify" {
		installTestAuditKey(t, store)
	}
	machine := recoverMachineOnStore(t, store)
	// Production startup restores both FSM metadata and the persisted cache
	// before replaying WAL entries whose plans may declare cache hits.
	require.NoError(t, NewRecovery(machine, store).RestoreCacheFromStore())
	if phase == "resume" {
		require.Equal(t, uint64(2), machine.State.LastAppliedIndex)
	}
	if phase == "verify" {
		require.Equal(t, uint64(5), machine.State.LastAppliedIndex)
	}
	if machine.State.LastAppliedIndex == 0 {
		installTestClusterPolicy(machine)
	}
	policy := machine.State.ClusterPolicy.CloneVT()
	policy.Revision = 2
	policy.IdempotencyTtlMicros = 9000
	nextPolicy := policy.CloneVT()
	nextPolicy.Revision = 3
	nextPolicy.IdempotencyTtlMicros = 10000
	policyOrder := func(p *commonpb.ClusterPolicy) *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{Payload: &raftcmdpb.SystemScopedOrder_SetClusterPolicy{
			SetClusterPolicy: &raftcmdpb.SetClusterPolicyOrder{Policy: p},
		}}}}
	}
	orders := []*raftcmdpb.Order{
		policyOrder(policy),
		createLedgerOrder("compatibility"),
		createLedgerOrder("compatibility"), // Audit a business rejection too.
		policyOrder(nextPolicy),
		createLedgerOrder("after-upgrade"),
	}
	last, err := log.LastIndex()
	require.NoError(t, err)
	end := uint64(len(orders))
	if phase == "seed" {
		end = 3
	}
	if phase == "resume" {
		require.Equal(t, uint64(3), last)
	}
	if phase == "verify" {
		require.Equal(t, end, last)
	}
	for index := last + 1; index <= end; index++ {
		data := compatibilityProposalBytes(t, index, orders[index-1], os.Getenv("LEDGER_PROPOSAL_PROBE_ENCODING") == "payload")
		entry := &raftpb.Entry{Index: new(index), Term: new(uint64(1)), Type: new(raftpb.EntryNormal), Data: data}
		require.NoError(t, log.Append(&raftpb.HardState{Term: new(uint64(1)), Commit: new(index)}, []*raftpb.Entry{entry}))
	}
	appliedEnd := end
	if phase == "seed" {
		appliedEnd = 2
	} // Leave a committed legacy WAL entry for the new process to replay.
	if machine.State.LastAppliedIndex < appliedEnd {
		entries, err := log.Entries(machine.State.LastAppliedIndex+1, appliedEnd+1, math.MaxUint64)
		require.NoError(t, err)
		for _, entry := range entries {
			result, err := machine.ApplyEntries(t.Context(), store, entry)
			require.NoError(t, err)
			require.True(t, result.Results[0].AuditEntryWritten)
		}
	}
	var report struct {
		Policy    *commonpb.ClusterPolicy
		Ledgers   []*commonpb.LedgerInfo
		Audit     []*auditpb.AuditEntry
		Items     [][]*auditpb.AuditItem
		Intent    [][]*auditpb.AuditItem
		Applied   uint64
		Timestamp uint64
	}
	reader, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	for _, name := range []string{"compatibility", "after-upgrade"} {
		if phase == "seed" && name == "after-upgrade" {
			continue
		}
		ledger, err := query.GetLedgerByName(t.Context(), reader, name)
		require.NoError(t, err)
		report.Ledgers = append(report.Ledgers, ledger)
	}
	var previousHash []byte
	for sequence := uint64(1); sequence < machine.State.NextAuditSequenceID; sequence++ {
		audit, err := query.ReadAuditEntry(t.Context(), reader, sequence)
		require.NoError(t, err)
		items, err := query.ReadAuditItems(t.Context(), reader, sequence)
		require.NoError(t, err)
		// Check every chain link in both binaries, including the native suffix.
		header, err := BuildHashedHeaderPayload(audit)
		require.NoError(t, err)
		slices := [][]byte{header}
		normalized := make([]*auditpb.AuditItem, len(items))
		for i, item := range items {
			slices = append(slices, BuildPerItemPayload(item))
			normalized[i] = item.CloneVT()
			// Decode field 4 by its published wire number so this exact test source
			// also compiles against RC.1. Normalize intent only in the test report;
			// the separate Items report retains the real persisted evidence.
			encoded, err := item.MarshalVT()
			require.NoError(t, err)
			for rest := encoded; len(rest) > 0; {
				number, typ, n := protowire.ConsumeTag(rest)
				require.Positive(t, n)
				size := protowire.ConsumeFieldValue(number, typ, rest[n:])
				require.GreaterOrEqual(t, size, 0)
				if number == 4 {
					raw, consumed := protowire.ConsumeBytes(rest[n:])
					require.Positive(t, consumed)
					control := &raftcmdpb.SetClusterPolicyOrder{}
					require.NoError(t, control.UnmarshalVT(raw))
					bytes, err := policyOrder(control.GetPolicy()).MarshalVT()
					require.NoError(t, err)
					normalized[i] = &auditpb.AuditItem{OrderIndex: item.GetOrderIndex(), LogSequence: item.GetLogSequence(), SerializedOrder: bytes}
				}
				rest = rest[n+size:]
			}
		}
		generator := processing.NewHashGenerator(commonpb.HashAlgorithm(audit.GetHashVersion()), machine.State.AuditKey)
		_, actualHash := generator.Compute(nil, previousHash, slices)
		require.Equal(t, audit.GetHash(), actualHash)
		previousHash = audit.GetHash()
		report.Intent = append(report.Intent, normalized)
		report.Audit = append(report.Audit, audit)
		report.Items = append(report.Items, items)
	}
	report.Policy = machine.State.ClusterPolicy
	report.Applied = machine.State.LastAppliedIndex
	report.Timestamp = machine.State.LastAppliedTimestamp
	data, err := json.Marshal(report)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(output, data, 0o600))
}

func compatibilityProposalBytes(t *testing.T, index uint64, order *raftcmdpb.Order, payload bool) []byte {
	t.Helper()
	p := sealProposal(makeProposal(index, order))
	data, err := p.MarshalVT()
	require.NoError(t, err)
	if payload {
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

	return data
}
