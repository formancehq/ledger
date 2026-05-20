package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

func TestProcessCreateLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}

	// Setup expectations
	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)
	mockStore.EXPECT().IncrementNextLedgerID().Return(uint32(1))
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any()).Do(
		func(name string, info *commonpb.LedgerInfo) {
			require.Equal(t, "test-ledger", info.GetName())
			require.Equal(t, now, info.GetCreatedAt())
			require.Equal(t, uint32(1), info.GetId(), "LedgerInfo should have Id == 1")
		},
	)
	mockStore.EXPECT().PutBoundaries("test-ledger", gomock.Any()).Do(
		func(ledger string, boundaries *raftcmdpb.LedgerBoundaries) {
			require.Equal(t, uint64(1), boundaries.GetNextTransactionId())
			require.Equal(t, uint64(1), boundaries.GetNextLogId())
		},
	)

	request := &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name: "test-ledger",
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	createLedgerLog := result.GetCreateLedger()
	require.NotNil(t, createLedgerLog)
	require.Equal(t, "test-ledger", createLedgerLog.GetName())
	require.Equal(t, uint32(1), createLedgerLog.GetId(), "CreateLedgerLog should have Id == 1")
}

func TestProcessCreateLedger_AlreadyExists(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	existingLedger := &commonpb.LedgerInfo{Name: "test-ledger"}
	mockStore.EXPECT().GetLedger("test-ledger").Return(existingLedger, true)

	request := &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name: "test-ledger",
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "ledger already exists")
}

func TestProcessDeleteLedger(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	now := &commonpb.Timestamp{Data: 1234567890}
	existingLedger := &commonpb.LedgerInfo{Name: "test-ledger"}

	mockStore.EXPECT().GetLedger("test-ledger").Return(existingLedger, true)
	mockStore.EXPECT().GetDate().Return(now)
	mockStore.EXPECT().PutLedger("test-ledger", gomock.Any())
	mockStore.EXPECT().MarkLedgerForCleanup("test-ledger")

	request := &servicepb.Request{
		Type: &servicepb.Request_DeleteLedger{
			DeleteLedger: &servicepb.DeleteLedgerRequest{
				Name: "test-ledger",
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	deleteLedgerLog := result.GetDeleteLedger()
	require.NotNil(t, deleteLedgerLog)
	require.Equal(t, "test-ledger", deleteLedgerLog.GetName())
	require.Equal(t, now, deleteLedgerLog.GetDeletedAt())
}

func TestProcessDeleteLedger_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().GetLedger("test-ledger").Return(nil, false)

	request := &servicepb.Request{
		Type: &servicepb.Request_DeleteLedger{
			DeleteLedger: &servicepb.DeleteLedgerRequest{
				Name: "test-ledger",
			},
		},
	}

	result, err := processor.ProcessOrder(requestToOrder(request), mockStore)
	require.Error(t, err)
	require.Nil(t, result)
	require.Contains(t, err.Error(), "ledger does not exist")
}
