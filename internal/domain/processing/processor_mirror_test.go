package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestMirrorIngest_FillGap(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	// Contiguous prefix: v2LogId 5 requires the applied prefix to be at 4.
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LastMirrorV2LogId: 4}
	ledgerInfo := &commonpb.LedgerInfo{
		Name: "mirror-ledger",
		Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
	}

	var putBoundaries *raftcmdpb.LedgerBoundaries

	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo)
	mockStore.EXPECT().GetDate().Return(now.AsReader())

	boundariesStub := setupBoundariesStub(mockStore)
	boundariesStub.expectGet(domain.LedgerKey{Name: "mirror-ledger"}, boundaries.AsReader(), nil)
	boundariesStub.onPut(func(_ domain.LedgerKey, b *raftcmdpb.LedgerBoundaries) { putBoundaries = b })

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "mirror-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
						V2LogId: 5,
						Data: &raftcmdpb.MirrorLogEntry_FillGap{
							FillGap: &raftcmdpb.MirrorFillGap{
								SkippedTransactionIds: []uint64{10, 11},
							},
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)
	require.Equal(t, "mirror-ledger", applyLog.GetLedgerName())
	require.Equal(t, uint64(1), applyLog.GetLog().GetId())

	fillGap := applyLog.GetLog().GetData().GetFillGap()
	require.NotNil(t, fillGap)
	require.Equal(t, uint64(5), fillGap.GetOriginalId())

	// NextTransactionId advances past the skipped IDs by value (max id + 1),
	// not by count — so skipping 10 and 11 moves the boundary to 12.
	require.NotNil(t, putBoundaries)
	require.Equal(t, uint64(12), putBoundaries.GetNextTransactionId())
}

func TestMirrorIngest_CreatedTransaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{
		Name: "mirror-ledger",
		Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
	}

	var putBoundaries *raftcmdpb.LedgerBoundaries

	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo)
	mockStore.EXPECT().GetDate().Return(now.AsReader()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(100))
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)

	boundariesStub := setupBoundariesStub(mockStore)
	boundariesStub.expectGet(domain.LedgerKey{Name: "mirror-ledger"}, boundaries.AsReader(), nil)
	boundariesStub.onPut(func(_ domain.LedgerKey, b *raftcmdpb.LedgerBoundaries) { putBoundaries = b })

	// Expect volume operations for source and destination (force=true, no balance checks)
	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}
	volumes := setupVolumesStub(mockStore)
	volumes.expectGet(domain.NewVolumeKey("mirror-ledger", "world", "USD/2"), zeroVol.AsReader(), nil)
	volumes.expectGet(domain.NewVolumeKey("mirror-ledger", "users:001", "USD/2"), zeroVol.AsReader(), nil)

	// Transaction state update
	expectPutTransactionState(t, mockStore,
		domain.TransactionKey{LedgerName: "mirror-ledger", ID: 42}, nil)

	// Reference storage
	expectPutTransactionReference(t, mockStore,
		domain.TransactionReferenceKey{LedgerName: "mirror-ledger", Reference: "tx-ref-v2"}, nil)

	postings := []*commonpb.Posting{{
		Source:      "world",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD/2",
	}}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "mirror-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
						V2LogId: 1,
						Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
							CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
								TransactionId: 42,
								Postings:      postings,
								Reference:     "tx-ref-v2",
								Timestamp:     &commonpb.Timestamp{Data: 1700000000},
							},
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	applyLog := result.GetApply()
	require.NotNil(t, applyLog)

	createdTx := applyLog.GetLog().GetData().GetCreatedTransaction()
	require.NotNil(t, createdTx)
	require.Equal(t, uint64(42), createdTx.GetTransaction().GetId())
	require.Equal(t, "tx-ref-v2", createdTx.GetTransaction().GetReference())

	// NextTransactionId should be past 42
	require.NotNil(t, putBoundaries)
	require.Equal(t, uint64(43), putBoundaries.GetNextTransactionId())
}

// mirrorCreatedTxOrder builds a MirrorIngest order for a created transaction
// carrying the given v2LogId. Shared by the idempotency tests so they exercise
// the exact wrapper shape (v2LogId on MirrorLogEntry) the guard reads.
func mirrorCreatedTxOrder(ledger string, v2LogID, txID uint64) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
						V2LogId: v2LogID,
						Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
							CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
								TransactionId: txID,
								Postings: []*commonpb.Posting{{
									Source:      "world",
									Destination: "users:001",
									Amount:      commonpb.NewUint256FromUint64(500),
									Asset:       "USD/2",
								}},
							},
						},
					}},
				},
			},
		},
	}
}

// TestMirrorIngest_ReplayIsNoOp pins the EN-1550 idempotency guard: an ingest
// whose v2LogId equals the recorded high-water mark (LastMirrorV2LogId) has
// already been applied, so re-applying it is a deterministic side-effect-free
// no-op — no postings applied (no volume writes → balances not doubled), no
// ledger re-touch, no boundary advance, and (nil, nil) returned so ProcessOrders
// emits no log.
func TestMirrorIngest_ReplayIsNoOp(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	// v2LogId 7 was already applied: LastMirrorV2LogId == 7.
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 43, NextLogId: 5, LastMirrorV2LogId: 7}
	ledgerInfo := &commonpb.LedgerInfo{Name: "mirror-ledger", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()

	boundariesStub := setupBoundariesStub(mockStore)
	boundariesStub.expectGet(domain.LedgerKey{Name: "mirror-ledger"}, boundaries.AsReader(), nil)
	// The guard runs before any mutation: no boundary Put on replay.
	boundariesStub.onPut(func(_ domain.LedgerKey, _ *raftcmdpb.LedgerBoundaries) {
		t.Errorf("Boundaries().Put must not be called on an idempotent replay")
	})
	// No ledger re-touch on replay (guarded before s.Ledgers().Put).
	ledgersStub := setupLedgersStub(mockStore)
	ledgersStub.onPut(func(_ domain.LedgerKey, _ *commonpb.LedgerInfo) {
		t.Errorf("Ledgers().Put must not be called on an idempotent replay")
	})
	// No posting applied → no volume writes → balances cannot double.
	volumesStub := setupVolumesStub(mockStore)
	volumesStub.onPut(func(_ domain.VolumeKey, _ *raftcmdpb.VolumePair) {
		t.Errorf("Volumes().Put must not be called on an idempotent replay")
	})

	// Replay v2LogId 7 (already applied).
	result, err := processor.ProcessOrder(mirrorCreatedTxOrder("mirror-ledger", 7, 42), mockStore)
	require.NoError(t, err)
	require.Nil(t, result, "replayed ingest must produce no log payload")
}

// TestMirrorIngest_LowerV2LogIdSkipped pins that an OLDER v2LogId arriving after
// a higher one (e.g. a tampered/rolled-back MirrorCursor makes the worker
// re-emit past logs) is skipped as a no-op.
func TestMirrorIngest_LowerV2LogIdSkipped(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 43, NextLogId: 5, LastMirrorV2LogId: 10}
	ledgerInfo := &commonpb.LedgerInfo{Name: "mirror-ledger", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()

	boundariesStub := setupBoundariesStub(mockStore)
	boundariesStub.expectGet(domain.LedgerKey{Name: "mirror-ledger"}, boundaries.AsReader(), nil)
	boundariesStub.onPut(func(_ domain.LedgerKey, _ *raftcmdpb.LedgerBoundaries) {
		t.Errorf("Boundaries().Put must not be called for a stale (lower) v2LogId")
	})
	volumesStub := setupVolumesStub(mockStore)
	volumesStub.onPut(func(_ domain.VolumeKey, _ *raftcmdpb.VolumePair) {
		t.Errorf("Volumes().Put must not be called for a stale (lower) v2LogId")
	})

	// v2LogId 6 is older than the applied high-water mark 10.
	result, err := processor.ProcessOrder(mirrorCreatedTxOrder("mirror-ledger", 6, 99), mockStore)
	require.NoError(t, err)
	require.Nil(t, result)
}

// TestMirrorIngest_AdvancesLastMirrorV2LogId pins that a forward ingest applies
// and records its v2LogId as the new high-water mark on the boundaries it writes.
func TestMirrorIngest_AdvancesLastMirrorV2LogId(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	// Previously applied up to v2LogId 3; the new entry (4) is strictly greater.
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LastMirrorV2LogId: 3}
	ledgerInfo := &commonpb.LedgerInfo{Name: "mirror-ledger", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}

	var putBoundaries *raftcmdpb.LedgerBoundaries

	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo)
	mockStore.EXPECT().GetDate().Return(now.AsReader()).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(100))
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)

	boundariesStub := setupBoundariesStub(mockStore)
	boundariesStub.expectGet(domain.LedgerKey{Name: "mirror-ledger"}, boundaries.AsReader(), nil)
	boundariesStub.onPut(func(_ domain.LedgerKey, b *raftcmdpb.LedgerBoundaries) { putBoundaries = b })

	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}
	volumes := setupVolumesStub(mockStore)
	volumes.expectGet(domain.NewVolumeKey("mirror-ledger", "world", "USD/2"), zeroVol.AsReader(), nil)
	volumes.expectGet(domain.NewVolumeKey("mirror-ledger", "users:001", "USD/2"), zeroVol.AsReader(), nil)
	expectPutTransactionState(t, mockStore, domain.TransactionKey{LedgerName: "mirror-ledger", ID: 42}, nil)

	result, err := processor.ProcessOrder(mirrorCreatedTxOrder("mirror-ledger", 4, 42), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	require.NotNil(t, putBoundaries)
	require.Equal(t, uint64(4), putBoundaries.GetLastMirrorV2LogId(),
		"applied ingest must advance the v2LogId high-water mark")
}

// TestMirrorIngest_GapRejected pins the contiguous-prefix invariant: an ingest
// whose v2LogId is beyond the next contiguous slot (last+1) is a gap — impossible
// in normal contiguous ingestion, so the FSM fails LOUD (ErrMirrorV2LogIDGap,
// KindInternal) and mutates nothing rather than silently applying past it.
func TestMirrorIngest_GapRejected(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	// Applied prefix at 3, so the next contiguous slot is 4; v2LogId 6 is a gap.
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1, LastMirrorV2LogId: 3}
	ledgerInfo := &commonpb.LedgerInfo{Name: "mirror-ledger", Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()

	boundariesStub := setupBoundariesStub(mockStore)
	boundariesStub.expectGet(domain.LedgerKey{Name: "mirror-ledger"}, boundaries.AsReader(), nil)
	// No mutation on a gap rejection: no boundary Put, no ledger re-touch, no volume Put.
	boundariesStub.onPut(func(_ domain.LedgerKey, _ *raftcmdpb.LedgerBoundaries) {
		t.Errorf("Boundaries().Put must not be called on a gap rejection")
	})
	ledgersStub := setupLedgersStub(mockStore)
	ledgersStub.onPut(func(_ domain.LedgerKey, _ *commonpb.LedgerInfo) {
		t.Errorf("Ledgers().Put must not be called on a gap rejection")
	})
	volumesStub := setupVolumesStub(mockStore)
	volumesStub.onPut(func(_ domain.VolumeKey, _ *raftcmdpb.VolumePair) {
		t.Errorf("Volumes().Put must not be called on a gap rejection")
	})

	result, err := processor.ProcessOrder(mirrorCreatedTxOrder("mirror-ledger", 6, 42), mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var gap *domain.ErrMirrorV2LogIDGap
	require.ErrorAs(t, err, &gap)
	require.Equal(t, "mirror-ledger", gap.Name)
	require.Equal(t, uint64(6), gap.Got)
	require.Equal(t, uint64(4), gap.Expected)
}

func TestMirrorIngest_NotMirrorMode(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	ledgerInfo := &commonpb.LedgerInfo{
		Name: "normal-ledger",
		Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL,
	}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "normal-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "normal-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
						V2LogId: 1,
						Data: &raftcmdpb.MirrorLogEntry_FillGap{
							FillGap: &raftcmdpb.MirrorFillGap{},
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var notMirror *domain.ErrLedgerNotInMirrorMode
	require.ErrorAs(t, err, &notMirror)
}

func TestMirrorIngest_LedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	expectGetLedger(mockStore, domain.LedgerKey{Name: "missing"}, nil, domain.ErrNotFound)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "missing",
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
						V2LogId: 1,
						Data: &raftcmdpb.MirrorLogEntry_FillGap{
							FillGap: &raftcmdpb.MirrorFillGap{},
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var ledgerNotFound *domain.ErrLedgerNotFound
	require.ErrorAs(t, err, &ledgerNotFound)
}

func TestPromoteLedger_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	ledgerInfo := &commonpb.LedgerInfo{
		Name: "mirror-ledger",
		Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
		MirrorSource: &commonpb.MirrorSourceConfig{
			LedgerName: "default",
			Type: &commonpb.MirrorSourceConfig_Http{
				Http: &commonpb.HttpMirrorSourceConfig{
					BaseUrl: "http://v2:3068",
				},
			},
		},
	}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil)
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "mirror-ledger"}, nil, func(_ string, info *commonpb.LedgerInfo) {
		require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_NORMAL, info.GetMode())
		require.Nil(t, info.GetMirrorSource())
	})

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "mirror-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{
					PromoteLedger: &raftcmdpb.PromoteLedgerOrder{},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	promoteLog := result.GetPromoteLedger()
	require.NotNil(t, promoteLog)
	require.Equal(t, "mirror-ledger", promoteLog.GetName())
}

func TestPromoteLedger_NotMirrorMode(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	ledgerInfo := &commonpb.LedgerInfo{
		Name: "normal-ledger",
		Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL,
	}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "normal-ledger"}, ledgerInfo.AsReader(), nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "normal-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{
					PromoteLedger: &raftcmdpb.PromoteLedgerOrder{},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var notMirror *domain.ErrLedgerNotInMirrorMode
	require.ErrorAs(t, err, &notMirror)
}

func TestPromoteLedger_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	expectGetLedger(mockStore, domain.LedgerKey{Name: "missing"}, nil, domain.ErrNotFound)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "missing",
				Payload: &raftcmdpb.LedgerScopedOrder_PromoteLedger{
					PromoteLedger: &raftcmdpb.PromoteLedgerOrder{},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var ledgerNotFound *domain.ErrLedgerNotFound
	require.ErrorAs(t, err, &ledgerNotFound)
}

// TestMirrorIngest_CreatedTransaction_AbsentVolumes pins the EN-1378
// contract through the mirror ingestion path: when source/destination
// volumes are declared but absent in the cache (Scope.GetVolume →
// domain.ErrNotFound), the mirror processor must auto-init them to zero
// and apply the posting — matching the explicit promise of
// processMirrorCreatedTransaction's doc-comment ("Missing volumes are
// auto-initialized to zero so postings are never silently skipped").
//
// Pre-EN-1378, "missing volume" was an error path (ErrBalanceNotPreloaded)
// asserted by this test; under the new contract it is the normal path —
// admission emits Declare for absent keys and readVolumeOrZero synthesises
// the zero at apply.
func TestMirrorIngest_CreatedTransaction_AbsentVolumes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	ledgerInfo := &commonpb.LedgerInfo{
		Name: "mirror-ledger",
		Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
	}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo)
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, boundaries.AsReader(), nil)

	now := &commonpb.Timestamp{Data: 1234567890}

	// Both source (world) and destination volumes are absent — admission
	// emitted Declare for both, the cache has nothing, Volumes().Get
	// falls through to the kindStub's default ErrNotFound, and
	// readVolumeOrZero synthesises a zero balance. expectPutVolume both
	// wires the stub lazily AND pins that the apply path writes both
	// fresh balances back through Scope.Volumes().Put.
	expectPutVolume(t, mockStore, domain.NewVolumeKey("mirror-ledger", "world", "USD/2"), nil)
	expectPutVolume(t, mockStore, domain.NewVolumeKey("mirror-ledger", "users:rare-account", "USD/2"), nil)

	expectPutTransactionState(t, mockStore,
		domain.TransactionKey{LedgerName: "mirror-ledger", ID: 42}, nil)
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "mirror-ledger"}, nil)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().GetDate().Return(now.AsReader()).AnyTimes()
	mockStore.EXPECT().GetCurrentOpenChapter().Return(nil, false)

	postings := []*commonpb.Posting{{
		Source:      "world",
		Destination: "users:rare-account",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD/2",
	}}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "mirror-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
						V2LogId: 1,
						Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
							CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
								TransactionId: 42,
								Postings:      postings,
							},
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

// TestMirrorIngest_RevertedTransaction_AbsentVolumes pins the EN-1378
// contract for the mirror revert path: absent volumes are auto-initialised
// to zero and the revert posting applies (force mode skips the balance
// check, so even the non-world source on a zero balance is allowed).
func TestMirrorIngest_RevertedTransaction_AbsentVolumes(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	ledgerInfo := &commonpb.LedgerInfo{
		Name: "mirror-ledger",
		Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
	}
	// Contiguous prefix: v2LogId 2 requires the applied prefix to be at 1.
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 1, LastMirrorV2LogId: 1}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo)
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, boundaries.AsReader(), nil)

	now := &commonpb.Timestamp{Data: 1234567890}

	mockStore.EXPECT().PutReverted(domain.TransactionKey{LedgerName: "mirror-ledger", ID: 5}, true)
	expectGetTransactionState(mockStore, domain.TransactionKey{LedgerName: "mirror-ledger", ID: 5}, nil, domain.ErrNotFound)
	expectPutVolume(t, mockStore, domain.NewVolumeKey("mirror-ledger", "users:rare-account", "USD/2"), nil)
	expectPutVolume(t, mockStore, domain.NewVolumeKey("mirror-ledger", "world", "USD/2"), nil)
	expectPutTransactionState(t, mockStore,
		domain.TransactionKey{LedgerName: "mirror-ledger", ID: 42}, nil, func(_ domain.TransactionKey, st *commonpb.TransactionState) {
			require.Equal(t, uint64(5), st.GetRevertsTransaction())
		})
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "mirror-ledger"}, nil)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().GetDate().Return(now.AsReader()).AnyTimes()

	reversePostings := []*commonpb.Posting{{
		Source:      "users:rare-account",
		Destination: "world",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD/2",
	}}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "mirror-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
						V2LogId: 2,
						Data: &raftcmdpb.MirrorLogEntry_RevertedTransaction{
							RevertedTransaction: &raftcmdpb.MirrorRevertedTransaction{
								RevertedTransactionId: 5,
								NewTransactionId:      42,
								ReversePostings:       reversePostings,
							},
						},
					},
					},
				},
			},
		},
	}

	_, err = processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
}

// When the reverted original is present in the FSM cache, the mirror path records
// the reversion on it (reverted_by_transaction + reverted_at) just like the
// non-mirror revert, keeping the read representation identical across nodes.
func TestMirrorIngest_RevertedTransaction_LinksOriginal(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	ledgerInfo := &commonpb.LedgerInfo{
		Name: "mirror-ledger",
		Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
	}
	// Contiguous prefix: v2LogId 2 requires the applied prefix to be at 1.
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 1, LastMirrorV2LogId: 1}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo)
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, boundaries.AsReader(), nil)

	revertTimestamp := &commonpb.Timestamp{Data: 1234567890}

	origKey := domain.TransactionKey{LedgerName: "mirror-ledger", ID: 5}

	mockStore.EXPECT().PutReverted(origKey, true)
	expectGetTransactionState(mockStore, origKey, (&commonpb.TransactionState{CreatedByLog: 7}).AsReader(), nil)
	expectPutVolume(t, mockStore, domain.NewVolumeKey("mirror-ledger", "users:rare-account", "USD/2"), nil)
	expectPutVolume(t, mockStore, domain.NewVolumeKey("mirror-ledger", "world", "USD/2"), nil)
	expectPutTransactionState(t, mockStore, origKey, nil, func(_ domain.TransactionKey, st *commonpb.TransactionState) {
		require.Equal(t, uint64(42), st.GetRevertedByTransaction())
		require.Equal(t, revertTimestamp, st.GetRevertedAt())
	})
	expectPutTransactionState(t, mockStore,
		domain.TransactionKey{LedgerName: "mirror-ledger", ID: 42}, nil, func(_ domain.TransactionKey, st *commonpb.TransactionState) {
			require.Equal(t, uint64(5), st.GetRevertsTransaction())
		})
	expectPutBoundaries(t, mockStore, domain.LedgerKey{Name: "mirror-ledger"}, nil)
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(1))
	mockStore.EXPECT().GetDate().Return(revertTimestamp.AsReader()).AnyTimes()

	reversePostings := []*commonpb.Posting{{
		Source:      "users:rare-account",
		Destination: "world",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD/2",
	}}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "mirror-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
						V2LogId: 2,
						Data: &raftcmdpb.MirrorLogEntry_RevertedTransaction{
							RevertedTransaction: &raftcmdpb.MirrorRevertedTransaction{
								RevertedTransactionId: 5,
								NewTransactionId:      42,
								ReversePostings:       reversePostings,
							},
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)

	revertedTx := result.GetApply().GetLog().GetData().GetRevertedTransaction()
	require.Equal(t, uint64(5), revertedTx.GetRevertedTransactionId())
	require.Equal(t, uint64(5), revertedTx.GetRevertTransaction().GetRevertsTransaction())
}

func TestWriteGuard_MirrorModeBlocksApply(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{
		Name: "mirror-ledger",
		Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
	}

	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, boundaries.AsReader(), nil)
	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "mirror-ledger",
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
						CreateTransaction: &raftcmdpb.CreateTransactionOrder{
							Postings: []*commonpb.Posting{{
								Source:      "world",
								Destination: "users:001",
								Amount:      commonpb.NewUint256FromUint64(100),
								Asset:       "USD/2",
							}},
						},
					},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var mirrorErr *domain.ErrLedgerInMirrorMode
	require.ErrorAs(t, err, &mirrorErr)
	require.Equal(t, "mirror-ledger", mirrorErr.Name)
}
