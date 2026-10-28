package check

import (
	"context"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func auditBoundaryFillGap(ledger string, sourceID uint64, skipped ...uint64) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger: ledger,
		Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{
			Entry: &raftcmdpb.MirrorLogEntry{
				V2LogId: sourceID,
				Data: &raftcmdpb.MirrorLogEntry_FillGap{FillGap: &raftcmdpb.MirrorFillGap{
					SkippedTransactionIds: skipped,
				}},
			},
		}},
	}}}
}

func TestCheckAuditBoundaryMirrorLifecycle(t *testing.T) {
	t.Parallel()
	for _, scenario := range []string{"fill_gap", "no_op", "rejected_gap", "rejected_overflow", "deleted", "incomplete_chain"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()
			machine, store, attrs := newFSMPurgeMachine(t)
			applyFSMPurgeProposal(t, machine, store, attrs, 1, createMirrorLedgerOrder("mirror"))
			applyFSMPurgeProposal(t, machine, store, attrs, 2, auditBoundaryFillGap("mirror", 1, 10))
			require.Empty(t, runFSMPurgeCheck(t, store, attrs))
			switch scenario {
			case "no_op":
				// Rejected duplicate effects in the same successful proposal must
				// not become authoritative simply because another order is fresh.
				applyFSMPurgeProposal(t, machine, store, attrs, 3,
					auditBoundaryFillGap("mirror", 1, 99), auditBoundaryFillGap("mirror", 2, 10))
			case "rejected_gap":
				result := applyFSMPurgeProposalResult(t, machine, store, attrs, 3, auditBoundaryFillGap("mirror", 3, 99))
				require.Len(t, result.Results, 1)
				var gap *domain.ErrMirrorV2LogIDGap
				require.ErrorAs(t, result.Results[0].Error, &gap)
				require.Equal(t, uint64(2), gap.Expected)
			case "rejected_overflow":
				result := applyFSMPurgeProposalResult(t, machine, store, attrs, 3, auditBoundaryFillGap("mirror", 2, math.MaxUint64))
				require.Len(t, result.Results, 1)
				var exhausted *domain.ErrSequenceExhausted
				require.ErrorAs(t, result.Results[0].Error, &exhausted)
				require.Equal(t, domain.SequenceCounterTransactionID, exhausted.Counter)
			case "deleted":
				applyFSMPurgeProposal(t, machine, store, attrs, 3, &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "mirror", Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedger{DeleteLedger: &raftcmdpb.DeleteLedgerOrder{}}},
				}})
			case "incomplete_chain":
				reader, err := store.NewReadHandle()
				require.NoError(t, err)
				entry, err := query.ReadAuditEntry(context.Background(), reader, 2)
				require.NoError(t, err)
				require.NoError(t, reader.Close())
				entry.Timestamp = &commonpb.Timestamp{Data: 42}
				batch := store.OpenWriteSession()
				key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAudit).PutUint64(2).Build()
				require.NoError(t, batch.SetProto(key, entry))
				require.NoError(t, batch.Commit())
			}
			findings := runFSMPurgeCheck(t, store, attrs)
			if scenario == "incomplete_chain" {
				require.NotEmpty(t, errorsOfType(findings, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH))
				require.Empty(t, errorsOfType(findings, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_BOUNDARY_MISMATCH), "a verified prefix is not final state")
			} else {
				require.Empty(t, findings)
			}
			reader, err := store.NewReadHandle()
			require.NoError(t, err)
			boundary, err := attrs.Boundary.Get(reader, domain.LedgerKey{Name: "mirror"}.Bytes())
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			if scenario == "deleted" {
				require.Nil(t, boundary)
			} else {
				require.Equal(t, uint64(11), boundary.GetNextTransactionId())
			}
		})
	}
}

// Each case starts from a real committed history. The two corrupt projections
// must never be able to justify one another without changing the audit chain.
func TestCheckAuditBoundaryCorruption(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name         string
		orphan       bool
		boundary     bool
		wantHash     int
		wantBoundary int
	}{
		{name: "healthy"},
		{name: "boundary_only", boundary: true, wantBoundary: 1},
		{name: "orphan_only", orphan: true, wantHash: 1},
		{name: "coordinated", orphan: true, boundary: true, wantHash: 1, wantBoundary: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			machine, store, attrs := newFSMPurgeMachine(t)
			applyFSMPurgeProposal(t, machine, store, attrs, 1, createMirrorLedgerOrder("mirror"))
			require.Empty(t, runFSMPurgeCheck(t, store, attrs), "healthy full-Check control")

			reader, err := store.NewReadHandle()
			require.NoError(t, err)
			boundary, err := attrs.Boundary.Get(reader, domain.LedgerKey{Name: "mirror"}.Bytes())
			require.NoError(t, err)
			require.NoError(t, reader.Close())
			require.Equal(t, uint64(1), boundary.GetNextTransactionId())
			require.Equal(t, uint64(1), boundary.GetNextLogId())
			require.Zero(t, boundary.GetLastMirrorV2LogId())

			if tc.orphan {
				serialized, err := auditBoundaryFillGap("mirror", 1, 99).MarshalVT()
				require.NoError(t, err)
				item := &auditpb.AuditItem{LogSequence: 1, SerializedOrder: serialized}
				value, err := item.MarshalVT()
				require.NoError(t, err)
				batch := store.OpenWriteSession()
				key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).
					PutUint64(99).PutUint32(0).Build()
				require.NoError(t, batch.SetBytes(key, value))
				require.NoError(t, batch.Commit())
			}
			if tc.boundary {
				boundary.NextTransactionId = 100
				writeBoundaries(t, store, attrs, "mirror", boundary)
			}

			findings := runFSMPurgeCheck(t, store, attrs)
			hash := errorsOfType(findings, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH)
			boundaries := errorsOfType(findings, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_BOUNDARY_MISMATCH)
			require.Len(t, hash, tc.wantHash, "all findings: %v", findings)
			require.Len(t, boundaries, tc.wantBoundary, "all findings: %v", findings)
			require.Len(t, findings, tc.wantHash+tc.wantBoundary)
			if tc.wantBoundary != 0 {
				require.Contains(t, boundaries[0].GetMessage(), "nextTransactionId mismatch: stored 100, expected 1")
			}
		})
	}
}

func TestCheckAuditItemSurplusUnderParent(t *testing.T) {
	t.Parallel()
	machine, store, attrs := newFSMPurgeMachine(t)
	applyFSMPurgeProposal(t, machine, store, attrs, 1, createMirrorLedgerOrder("mirror"))
	require.Empty(t, runFSMPurgeCheck(t, store, attrs))

	serialized, err := auditBoundaryFillGap("mirror", 1, 99).MarshalVT()
	require.NoError(t, err)
	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).
		PutUint64(1).PutUint32(1).Build()
	require.NoError(t, batch.SetProto(key, &auditpb.AuditItem{OrderIndex: 1, LogSequence: 1, SerializedOrder: serialized}))
	require.NoError(t, batch.Commit())

	findings := runFSMPurgeCheck(t, store, attrs)
	hashes := errorsOfType(findings, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_HASH_MISMATCH)
	require.Len(t, hashes, 1, "a structurally valid item still needs hash verification: %v", findings)
	require.NotContains(t, hashes[0].GetMessage(), "physical identity or parent membership")
	require.Contains(t, hashes[0].GetMessage(), "audit hash chain broken at sequence 1")
}
