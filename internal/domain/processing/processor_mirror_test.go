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
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
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

	// NextTransactionId should have advanced by 2 (two skipped IDs)
	require.NotNil(t, putBoundaries)
	require.Equal(t, uint64(3), putBoundaries.GetNextTransactionId())
}

func TestMirrorIngest_CreatedTransaction(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	allowAccountMarkers(mockStore)
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
	allowAccountMarkers(mockStore)
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
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 10, NextLogId: 1}

	expectGetLedger(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo.AsReader(), nil).AnyTimes()
	expectPutLedger(t, mockStore, domain.LedgerKey{Name: "mirror-ledger"}, ledgerInfo)
	expectGetBoundaries(mockStore, domain.LedgerKey{Name: "mirror-ledger"}, boundaries.AsReader(), nil)

	now := &commonpb.Timestamp{Data: 1234567890}

	mockStore.EXPECT().PutReverted(domain.TransactionKey{LedgerName: "mirror-ledger", ID: 5}, true)
	expectGetTransactionState(mockStore, domain.TransactionKey{LedgerName: "mirror-ledger", ID: 5}, nil, domain.ErrNotFound)
	expectPutVolume(t, mockStore, domain.NewVolumeKey("mirror-ledger", "users:rare-account", "USD/2"), nil)
	expectPutVolume(t, mockStore, domain.NewVolumeKey("mirror-ledger", "world", "USD/2"), nil)
	expectPutTransactionState(t, mockStore,
		domain.TransactionKey{LedgerName: "mirror-ledger", ID: 42}, nil)
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
