package state

// This probe deliberately uses only the RC.3 API. scripts/check-proposal-payload
// compiles this same source against the released tree and the candidate. It
// checks the same expected FSM/audit behavior in separate binaries, not two
// modes of a single decoder. Numeric wire tags below are the format oracle.

import (
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
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

func TestProposalPayloadCompatibilityProbe(t *testing.T) {
	if os.Getenv("LEDGER_PROPOSAL_PROBE_RUN") != "1" {
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
			decoded := &raftcmdpb.Proposal{}
			require.NoError(t, decoded.UnmarshalVT(entry.GetData()))
			// RC.3 sees only the legacy orders; native controls contain none.
			nativePolicy := len(decoded.GetOrders()) == 0 && (entry.GetIndex() == 1 || entry.GetIndex() == 4)
			require.Equal(t, !nativePolicy, result.Results[0].AuditEntryWritten)
		}
	}
	reader, err := store.NewDirectReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()
	ledgerNames := []string{"compatibility", "after-upgrade"}
	if phase == "seed" {
		ledgerNames = ledgerNames[:1]
	}
	for _, name := range ledgerNames {
		ledger, err := query.GetLedgerByName(t.Context(), reader, name)
		require.NoError(t, err)
		require.Equal(t, name, ledger.GetName())
	}
	var previousHash []byte
	var historicalAuditHashes []byte
	auditCount := 0
	for sequence := uint64(1); sequence < machine.State.NextAuditSequenceID; sequence++ {
		audit, err := query.ReadAuditEntry(t.Context(), reader, sequence)
		require.NoError(t, err)
		items, err := query.ReadAuditItems(t.Context(), reader, sequence)
		require.NoError(t, err)
		// Check every chain link in both binaries, including the native suffix.
		header, err := BuildHashedHeaderPayload(audit)
		require.NoError(t, err)
		slices := [][]byte{header}
		for _, item := range items {
			slices = append(slices, BuildPerItemPayload(item))
		}
		generator := processing.NewHashGenerator(commonpb.HashAlgorithm(audit.GetHashVersion()), machine.State.AuditKey)
		_, actualHash := generator.Compute(nil, previousHash, slices)
		require.Equal(t, audit.GetHash(), actualHash)
		if sequence <= 3 {
			observedHash := audit.GetHash()
			if os.Getenv("LEDGER_PROPOSAL_PROBE_MUTATE_REJECTION") == "1" && sequence == 3 {
				// Negative control: model a changed rejection audit entry with a
				// valid recomputed hash. The independent RC.3 oracle must reject it.
				changed := audit.CloneVT()
				failure := changed.GetFailure()
				require.NotNil(t, failure)
				details := failure.GetDetails().GetLedgerAlreadyExists()
				require.NotNil(t, details)
				details.Name = new("changed-ledger")
				changedHeader, err := BuildHashedHeaderPayload(changed)
				require.NoError(t, err)
				changedSlices := append([][]byte{changedHeader}, slices[1:]...)
				_, observedHash = generator.Compute(nil, previousHash, changedSlices)
			}
			historicalAuditHashes = protowire.AppendVarint(historicalAuditHashes, sequence)
			historicalAuditHashes = protowire.AppendBytes(historicalAuditHashes, observedHash)
		}
		previousHash = audit.GetHash()
		auditCount++
	}
	expectedAudits := 5
	switch {
	case phase == "seed":
		expectedAudits = 2
	case phase == "resume" || phase == "verify":
		expectedAudits = 4
	case os.Getenv("LEDGER_PROPOSAL_PROBE_ENCODING") == "payload":
		expectedAudits = 3
	}
	require.Equal(t, expectedAudits, auditCount)
	expectedPolicy := compatibilityPolicyState{
		Revision:                    3,
		IdempotencyTtlMicros:        10000,
		QueryCheckpointLimit:        10,
		MetadataMaxEntriesPerEntity: 128,
		MetadataMaxKeyBytes:         256,
		MetadataMaxValueBytes:       16384,
		MetadataMaxEntityBytes:      65536,
		MetadataMaxCommandBytes:     1048576,
	}
	expectedApplied := uint64(5)
	if phase == "seed" {
		expectedPolicy.Revision = 2
		expectedPolicy.IdempotencyTtlMicros = 9000
		expectedApplied = 2
	}
	require.Equal(t, expectedPolicy, compatibilityPolicyStateFrom(machine.State.ClusterPolicy))
	require.Equal(t, expectedApplied, machine.State.LastAppliedIndex)
	require.Equal(t, 1700000000+expectedApplied, machine.State.LastAppliedTimestamp)
	if oraclePath := os.Getenv("LEDGER_PROPOSAL_PROBE_AUDIT_ORACLE"); oraclePath != "" {
		switch os.Getenv("LEDGER_PROPOSAL_PROBE_AUDIT_ORACLE_MODE") {
		case "write":
			require.NoError(t, os.WriteFile(oraclePath, historicalAuditHashes, 0o600))
		case "check":
			expectedHashes, err := os.ReadFile(oraclePath)
			require.NoError(t, err)
			require.Equal(t, expectedHashes, historicalAuditHashes, "historical RC.3 audit hashes diverged")
		default:
			t.Fatal("invalid audit oracle mode")
		}
	}
}

// Compare the RC.3 policy contract by value. Later policy fields may be added
// without changing the meaning of the fields already persisted by RC.3.
type compatibilityPolicyState struct {
	Revision                    uint64
	IdempotencyTtlMicros        uint64
	QueryCheckpointLimit        uint64
	MetadataMaxEntriesPerEntity uint64
	MetadataMaxKeyBytes         uint64
	MetadataMaxValueBytes       uint64
	MetadataMaxEntityBytes      uint64
	MetadataMaxCommandBytes     uint64
}

func compatibilityPolicyStateFrom(policy *commonpb.ClusterPolicy) compatibilityPolicyState {
	return compatibilityPolicyState{
		Revision:                    policy.GetRevision(),
		IdempotencyTtlMicros:        policy.GetIdempotencyTtlMicros(),
		QueryCheckpointLimit:        policy.GetQueryCheckpointLimit(),
		MetadataMaxEntriesPerEntity: policy.GetMetadataMaxEntriesPerEntity(),
		MetadataMaxKeyBytes:         policy.GetMetadataMaxKeyBytes(),
		MetadataMaxValueBytes:       policy.GetMetadataMaxValueBytes(),
		MetadataMaxEntityBytes:      policy.GetMetadataMaxEntityBytes(),
		MetadataMaxCommandBytes:     policy.GetMetadataMaxCommandBytes(),
	}
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
