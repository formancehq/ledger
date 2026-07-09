package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// Per-order idempotency dedup/replay/conflict has moved to the FSM apply path
// (covered by internal/infra/state/idempotency_apply_test.go); ProcessOrders no
// longer performs it, so those tests were removed.

// TestComputeOrderHash_ExcludesCoverageBits pins the contract that the
// idempotency hash is computed over only the user-supplied request content —
// never CoverageBits, which admission rebuilds from the proposal-wide
// ExecutionPlan. The same logical order in a different batch (and therefore a
// different CoverageBits) MUST hash identically; otherwise the idempotency
// check would reject a legitimate retry.
func TestHashOrders_ExcludesCoverageBits(t *testing.T) {
	t.Parallel()

	base := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "L",
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		},
	}
	baseHash := HashOrders([]*raftcmdpb.Order{base})

	withCoverage := &raftcmdpb.Order{
		Type:         base.GetType(),
		CoverageBits: []byte{0b0101},
	}
	require.Equal(t, baseHash, HashOrders([]*raftcmdpb.Order{withCoverage}),
		"CoverageBits must not change the idempotency hash")
}

// TestHashOrders_MatchesHashProposal pins the equivalence the integrity checker
// relies on: re-deriving a proposal's frozen hash via HashOrders (from the
// audit orders) must be byte-identical to the hot-path HashProposal.
func TestHashOrders_MatchesHashProposal(t *testing.T) {
	t.Parallel()

	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	orders := []*raftcmdpb.Order{
		{
			Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger:  "L",
					Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
				},
			},
			CoverageBits: []byte{0b1},
		},
		{
			Type: &raftcmdpb.Order_LedgerScoped{
				LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "M"},
			},
		},
	}

	require.Equal(t, processor.HashProposal(&raftcmdpb.Proposal{Orders: orders}), HashOrders(orders),
		"HashOrders must be byte-identical to HashProposal so the checker re-derives the frozen hash")
}

func TestProcessOrders_WithoutIdempotencyKey(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		},
		// No idempotency key
	}

	proposal := &raftcmdpb.Proposal{
		Id:     1,
		Orders: []*raftcmdpb.Order{order},
	}

	// No idempotency check should be made
	// Process the order normally
	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, domain.ErrNotFound)
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil)
	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(100))

	response, err := processor.ProcessOrders(proposal.GetOrders(), mockFactory(mockStore), noopSink{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Logs, 1)

	createdLog := response.Logs[0].GetCreatedLog()
	require.NotNil(t, createdLog)
	require.Equal(t, uint64(100), createdLog.GetSequence())
}

// TestCreateLedgerAndTransactInSameBatch verifies that a batch containing
// CreateLedger + CreateTransaction (on the same ledger) succeeds and that
// the transaction uses the correct LedgerID in its volume keys.
func TestCreateLedgerAndTransactInSameBatch(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	// Track the ledger info stored by CreateLedger so GetLedger can return it.
	var storedLedgerInfo *commonpb.LedgerInfo

	// Shared Ledgers stub: Get returns storedLedgerInfo (or ErrNotFound
	// before CreateLedger runs); Put captures the info written by
	// CreateLedger so the subsequent CreateTransaction order sees it.
	ledgers := setupLedgersStub(mockStore)
	ledgers.onGet(func(_ domain.LedgerKey) (commonpb.LedgerInfoReader, error) {
		if storedLedgerInfo == nil {
			return nil, domain.ErrNotFound
		}

		return storedLedgerInfo.AsReader(), nil
	})
	ledgers.onPut(func(_ domain.LedgerKey, info *commonpb.LedgerInfo) {
		storedLedgerInfo = info
	})

	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now.AsReader()).AnyTimes()

	// Boundaries: CreateLedger Puts initial boundaries, then CreateTransaction
	// Gets/Puts them. A single shared stub serves both calls; the Get hook
	// returns the stored boundaries (post-CreateLedger).
	var storedBoundaries = &raftcmdpb.LedgerBoundaries{
		NextTransactionId: 1,
		NextLogId:         1,
	}
	boundaries := setupBoundariesStub(mockStore)
	boundaries.expectGet(domain.LedgerKey{Name: "myled"}, storedBoundaries.AsReader(), nil)
	boundaries.onPut(func(_ domain.LedgerKey, b *raftcmdpb.LedgerBoundaries) {
		storedBoundaries = b
	})

	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)

	// Volume operations: the LedgerID should be 1 (assigned by CreateLedger).
	srcKey := domain.NewVolumeKey("myled", "world", "USD")
	dstKey := domain.NewVolumeKey("myled", "users:bob", "USD")

	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	volumes := setupVolumesStub(mockStore)
	volumes.expectGet(srcKey, zeroVol.AsReader(), nil)
	volumes.expectGet(dstKey, zeroVol.AsReader(), nil)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	expectPutTransactionState(t, mockStore, domain.TransactionKey{LedgerName: "myled", ID: 1}, nil)

	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(2))

	orders := []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "myled",
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{},
				},
			},
		}},
		{Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "myled",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
						CreateTransaction: &raftcmdpb.CreateTransactionOrder{
							Postings: []*commonpb.Posting{
								{
									Source:      "world",
									Destination: "users:bob",
									Amount:      commonpb.NewUint256FromUint64(100),
									Asset:       "USD",
								},
							},
							Force: true,
						},
					},
					},
				},
			},
		}},
	}

	response, err := processor.ProcessOrders(orders, mockFactory(mockStore), noopSink{})
	require.NoError(t, err)
	require.Len(t, response.Logs, 2)

	// Verify order 1: CreateLedger log with Id=1.
	createLog := response.Logs[0].GetCreatedLog()
	require.NotNil(t, createLog)
	require.Equal(t, uint32(1), createLog.GetPayload().GetCreateLedger().GetId())

	// Verify order 2: CreateTransaction succeeded.
	txLog := response.Logs[1].GetCreatedLog()
	require.NotNil(t, txLog)
	applyLog := txLog.GetPayload().GetApply()
	require.NotNil(t, applyLog)
	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(1), createdTx.GetTransaction().GetId())
}

// TestProcessOrders_OrdersResultAccumulator pins the invariant that
// OrdersResult.{MinLogSequence,MaxLogSequence,CreatedLogs} are populated
// during the single per-order pass (no second walk needed in
// applyProposal). Previously these were re-derived from the Logs slice
// by extractLogSequenceRange (sequence range) and an inline filter
// (createdLogs rebuild) — both helpers are gone.
func TestProcessOrders_OrdersResultAccumulator(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1}

	// Two CreateLedger orders. Sequences assigned by IncrementNextSequenceID
	// are 100 and 110 — chosen non-contiguous so the test catches a min/max
	// confusion (an off-by-one or last-wins bug on min would still match if
	// the sequences were consecutive).
	expectGetLedger(mockStore, domain.LedgerKey{Name: "ledger-a"}, nil, domain.ErrNotFound)
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "ledger-a"}, nil)
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "ledger-a"}, nil)
	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(100))

	expectGetLedger(mockStore, domain.LedgerKey{Name: "ledger-b"}, nil, domain.ErrNotFound)
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(2))
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "ledger-b"}, nil)
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "ledger-b"}, nil)
	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(110))

	orders := []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger:  "ledger-a",
			Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
		}}},
		{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger:  "ledger-b",
			Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{}},
		}}},
	}

	response, err := processor.ProcessOrders(orders, mockFactory(mockStore), noopSink{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Len(t, response.Logs, 2)

	// CreatedLogs accumulated alongside Logs in the same pass.
	require.Len(t, response.CreatedLogs, 2)
	require.Equal(t, uint64(100), response.CreatedLogs[0].GetSequence())
	require.Equal(t, uint64(110), response.CreatedLogs[1].GetSequence())

	// Min/Max accumulated alongside Logs in the same pass.
	require.Equal(t, uint64(100), response.MinLogSequence)
	require.Equal(t, uint64(110), response.MaxLogSequence)
}

// TestProcessOrders_SkipOnReferenceConflict pins the ProcessOrders skip
// integration glue: matchOrderSkip, assignSkipLogIDAndDate and the
// overlay-scoped rollback are individually unit-tested, but the
// interleaving that ties them into a full skip cycle only surfaced under
// e2e. This test drives the full unit-level path: a duplicate reference
// triggers ErrTransactionReferenceConflict from processCreateTransaction,
// ProcessOrders converts it into an OrderSkippedLog, allocates its log
// id/date on the PARENT scope (not the overlay — the overlay is
// discarded), and stamps a sequence id from IncrementNextSequenceID.
func TestProcessOrders_SkipOnReferenceConflict(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 42}

	// Existing reference: the audit-bound claim recorded by an earlier
	// successful CreateTransaction. Its presence is the condition the
	// sub-processor detects to raise ErrTransactionReferenceConflict, and
	// its ExistingTransactionID surfaces on the skip's context so
	// callers can correlate without a follow-up read.
	existingRef := &commonpb.TransactionReferenceValue{TransactionId: 7}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "test-ledger"}, (&commonpb.LedgerInfo{Name: "test-ledger", Id: 1}).AsReader(), nil).AnyTimes()
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "test-ledger"}, boundaries.AsReader(), nil).AnyTimes()

	// The overlay pre-wraps every accessor at construction; register the
	// stubs even for kinds this test does not exercise so the eager
	// parent.<Kind>() calls don't fail as unexpected.
	stubs := stubsFor(mockStore)
	stubs.volumesStubFor(mockStore)
	stubs.accountMetadataStubFor(mockStore)
	stubs.transactionStatesStubFor(mockStore)
	stubs.indexesStubFor(mockStore)
	mockStore.EXPECT().LedgerMetadata().Return(&kindStub[domain.LedgerMetadataKey, *commonpb.MetadataValue, commonpb.MetadataValueReader]{}).AnyTimes()
	mockStore.EXPECT().PreparedQueries().Return(&kindStub[domain.PreparedQueryKey, *commonpb.PreparedQuery, commonpb.PreparedQueryReader]{}).AnyTimes()

	trStub, _ := stubs.transactionReferencesStubFor(mockStore)
	trStub.expectGet(domain.TransactionReferenceKey{LedgerName: "test-ledger", Reference: "ref-x"}, existingRef.AsReader(), nil)

	// assignSkipLogIDAndDate reads GetDate() for the skip log's Date and
	// bumps Boundaries.NextLogId on the parent — the boundary Put MUST
	// hit the parent (not the discarded overlay) so the ledger-local log
	// id sequence is preserved across the skip.
	mockStore.EXPECT().GetDate().Return(now.AsReader())
	var putBoundaries *raftcmdpb.LedgerBoundaries
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "test-ledger"}, nil, func(_ string, b *raftcmdpb.LedgerBoundaries) {
		putBoundaries = b
	})

	// Global proposal-level sequence ID for the skip log.
	mockStore.EXPECT().IncrementNextSequenceID().Return(uint64(100))

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "test-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
							CreateTransaction: &raftcmdpb.CreateTransactionOrder{
								Postings: []*commonpb.Posting{{
									Source:      "bank",
									Destination: "users:123",
									Amount:      commonpb.NewUint256FromUint64(100),
									Asset:       "USD",
								}},
								Reference: "ref-x",
							},
						},
						SkippableReasons: []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
					},
				},
			},
		},
	}

	response, err := processor.ProcessOrders([]*raftcmdpb.Order{order}, mockFactory(mockStore), noopSink{})
	require.NoError(t, err, "skip-tolerant order must not surface the whitelisted failure")
	require.NotNil(t, response)

	require.Len(t, response.CreatedLogs, 1, "the skip cycle must emit exactly one log")
	require.Equal(t, uint64(100), response.CreatedLogs[0].GetSequence(), "the skip log must carry the sequence id from IncrementNextSequenceID")

	require.Len(t, response.Logs, 1)
	created := response.Logs[0].GetCreatedLog()
	require.NotNil(t, created, "skip log must be materialised as CreatedLog (not a reference)")

	apply := created.GetPayload().GetApply()
	require.NotNil(t, apply)
	require.Equal(t, "test-ledger", apply.GetLedgerName())
	require.Equal(t, uint64(42), apply.GetLog().GetId(), "the skip log must consume the parent's NextLogId slot")
	require.NotNil(t, apply.GetLog().GetDate(), "the skip log must carry a Date stamped from the parent Scope")

	skipped := apply.GetLog().GetData().GetOrderSkipped()
	require.NotNil(t, skipped, "the log payload must be OrderSkipped, not the failed CreateTransaction")
	require.Equal(t, commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT, skipped.GetReason())
	require.Equal(t, "test-ledger", skipped.GetContext()["ledger"])
	require.Equal(t, "ref-x", skipped.GetContext()["reference"])

	// Parent boundary Put must reflect the incremented slot: the skip
	// consumed log id 42, next slot is 43.
	require.NotNil(t, putBoundaries)
	require.Equal(t, uint64(43), putBoundaries.GetNextLogId(), "parent NextLogId must advance past the skip")

	require.Equal(t, uint64(100), response.MinLogSequence)
	require.Equal(t, uint64(100), response.MaxLogSequence)
}

// TestProcessOrders_OrdersResultEmpty asserts the empty-batch sentinel:
// no orders → empty CreatedLogs, MinLogSequence == MaxLogSequence == 0.
// The AppliedProposal sync skips entries with MaxLogSequence == 0
// (cf. appliedProposalSync.advance), so the zero value is load-bearing.
func TestProcessOrders_OrdersResultEmpty(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	response, err := processor.ProcessOrders(nil, mockFactory(mockStore), noopSink{})
	require.NoError(t, err)
	require.NotNil(t, response)
	require.Empty(t, response.Logs)
	require.Empty(t, response.CreatedLogs)
	require.Equal(t, uint64(0), response.MinLogSequence)
	require.Equal(t, uint64(0), response.MaxLogSequence)
}
