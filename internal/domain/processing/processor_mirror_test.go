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

	mockStore.EXPECT().GetLedger("mirror-ledger").Return(ledgerInfo, nil).AnyTimes()
	mockStore.EXPECT().PutLedger("mirror-ledger", ledgerInfo)
	mockStore.EXPECT().GetBoundaries("mirror-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutBoundaries("mirror-ledger", gomock.Any()).Do(
		func(_ string, b *raftcmdpb.LedgerBoundaries) { putBoundaries = b },
	)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_MirrorIngest{
			MirrorIngest: &raftcmdpb.MirrorIngestOrder{
				Ledger: "mirror-ledger",
				Entry: &raftcmdpb.MirrorLogEntry{
					V2LogId: 5,
					Data: &raftcmdpb.MirrorLogEntry_FillGap{
						FillGap: &raftcmdpb.MirrorFillGap{
							SkippedTransactionIds: []uint64{10, 11},
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
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	boundaries := &raftcmdpb.LedgerBoundaries{NextTransactionId: 1, NextLogId: 1}
	ledgerInfo := &commonpb.LedgerInfo{
		Name: "mirror-ledger",
		Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
	}

	var putBoundaries *raftcmdpb.LedgerBoundaries

	mockStore.EXPECT().GetLedger("mirror-ledger").Return(ledgerInfo, nil).AnyTimes()
	mockStore.EXPECT().PutLedger("mirror-ledger", ledgerInfo)
	mockStore.EXPECT().GetBoundaries("mirror-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetDate().Return(now).AnyTimes()
	mockStore.EXPECT().GetNextSequenceID().Return(uint64(100))
	mockStore.EXPECT().GetCurrentOpenPeriod().Return(nil, false)
	mockStore.EXPECT().PutBoundaries("mirror-ledger", gomock.Any()).Do(
		func(_ string, b *raftcmdpb.LedgerBoundaries) { putBoundaries = b },
	)

	// Expect volume operations for source and destination (force=true, no balance checks)
	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}
	mockStore.EXPECT().GetVolume(gomock.Any()).Return(zeroVol.AsReader(), nil).Times(2)
	mockStore.EXPECT().PutVolume(gomock.Any(), gomock.Any()).Times(2)

	// Transaction state update
	mockStore.EXPECT().PutTransactionState(
		domain.TransactionKey{LedgerName: "mirror-ledger", ID: 42},
		gomock.Any(),
	)

	// Reference storage
	mockStore.EXPECT().PutTransactionReference(
		domain.TransactionReferenceKey{LedgerName: "mirror-ledger", Reference: "tx-ref-v2"},
		gomock.Any(),
	)

	postings := []*commonpb.Posting{{
		Source:      "world",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD/2",
	}}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_MirrorIngest{
			MirrorIngest: &raftcmdpb.MirrorIngestOrder{
				Ledger: "mirror-ledger",
				Entry: &raftcmdpb.MirrorLogEntry{
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

	mockStore.EXPECT().GetLedger("normal-ledger").Return(ledgerInfo, nil).AnyTimes()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_MirrorIngest{
			MirrorIngest: &raftcmdpb.MirrorIngestOrder{
				Ledger: "normal-ledger",
				Entry: &raftcmdpb.MirrorLogEntry{
					V2LogId: 1,
					Data: &raftcmdpb.MirrorLogEntry_FillGap{
						FillGap: &raftcmdpb.MirrorFillGap{},
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

	mockStore.EXPECT().GetLedger("missing").Return(nil, domain.ErrNotFound)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_MirrorIngest{
			MirrorIngest: &raftcmdpb.MirrorIngestOrder{
				Ledger: "missing",
				Entry: &raftcmdpb.MirrorLogEntry{
					V2LogId: 1,
					Data: &raftcmdpb.MirrorLogEntry_FillGap{
						FillGap: &raftcmdpb.MirrorFillGap{},
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

	mockStore.EXPECT().GetLedger("mirror-ledger").Return(ledgerInfo, nil)
	mockStore.EXPECT().PutLedger("mirror-ledger", gomock.Any()).Do(
		func(_ string, info *commonpb.LedgerInfo) {
			require.Equal(t, commonpb.LedgerMode_LEDGER_MODE_NORMAL, info.GetMode())
			require.Nil(t, info.GetMirrorSource())
		},
	)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_PromoteLedger{
			PromoteLedger: &raftcmdpb.PromoteLedgerOrder{
				Ledger: "mirror-ledger",
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

	mockStore.EXPECT().GetLedger("normal-ledger").Return(ledgerInfo, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_PromoteLedger{
			PromoteLedger: &raftcmdpb.PromoteLedgerOrder{
				Ledger: "normal-ledger",
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

	mockStore.EXPECT().GetLedger("missing").Return(nil, domain.ErrNotFound)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_PromoteLedger{
			PromoteLedger: &raftcmdpb.PromoteLedgerOrder{
				Ledger: "missing",
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var ledgerNotFound *domain.ErrLedgerNotFound
	require.ErrorAs(t, err, &ledgerNotFound)
}

// TestMirrorIngest_CreatedTransaction_MissingVolumes verifies that mirror mode
// rejects the command when volumes are not preloaded (indicating a preloading bug).
// Before the fix, applyPosting errors were silently ignored with `_ = applyPosting(...)`,
// resulting in lost volume updates. The fix propagates the error so the FSM rejects
// the command, making the preloading bug visible instead of silently corrupting data.
func TestMirrorIngest_CreatedTransaction_MissingVolumes(t *testing.T) {
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

	mockStore.EXPECT().GetLedger("mirror-ledger").Return(ledgerInfo, nil).AnyTimes()
	mockStore.EXPECT().PutLedger("mirror-ledger", ledgerInfo)
	mockStore.EXPECT().GetBoundaries("mirror-ledger").Return(boundaries.AsReader(), nil)

	// Simulate cache miss: GetVolume returns ErrNotFound for the source volume.
	// This happens when a volume was evicted from the dual-generation cache
	// and the preload didn't include it.
	mockStore.EXPECT().GetVolume(gomock.Any()).Return(nil, domain.ErrNotFound)

	postings := []*commonpb.Posting{{
		Source:      "world",
		Destination: "users:rare-account",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD/2",
	}}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_MirrorIngest{
			MirrorIngest: &raftcmdpb.MirrorIngestOrder{
				Ledger: "mirror-ledger",
				Entry: &raftcmdpb.MirrorLogEntry{
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
	}

	// The FSM must reject the command — not silently ignore the missing volume.
	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "not preloaded")
}

// TestMirrorIngest_RevertedTransaction_MissingVolumes verifies the same behavior
// for reverted transactions: the FSM must reject when volumes are not preloaded.
func TestMirrorIngest_RevertedTransaction_MissingVolumes(t *testing.T) {
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

	mockStore.EXPECT().GetLedger("mirror-ledger").Return(ledgerInfo, nil).AnyTimes()
	mockStore.EXPECT().PutLedger("mirror-ledger", ledgerInfo)
	mockStore.EXPECT().GetBoundaries("mirror-ledger").Return(boundaries.AsReader(), nil)

	// Simulate cache miss for volumes
	mockStore.EXPECT().GetVolume(gomock.Any()).Return(nil, domain.ErrNotFound)

	reversePostings := []*commonpb.Posting{{
		Source:      "users:rare-account",
		Destination: "world",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD/2",
	}}

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_MirrorIngest{
			MirrorIngest: &raftcmdpb.MirrorIngestOrder{
				Ledger: "mirror-ledger",
				Entry: &raftcmdpb.MirrorLogEntry{
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
	}

	// The FSM must reject the command — not silently ignore the missing volume.
	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "not preloaded")
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

	mockStore.EXPECT().GetBoundaries("mirror-ledger").Return(boundaries.AsReader(), nil)
	mockStore.EXPECT().GetLedger("mirror-ledger").Return(ledgerInfo, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "mirror-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
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
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var mirrorErr *domain.ErrLedgerInMirrorMode
	require.ErrorAs(t, err, &mirrorErr)
	require.Equal(t, "mirror-ledger", mirrorErr.Name)
}
