package processing

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestApplyPosting_WorldAccount_SkipsBalanceCheck(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{Ledger: "test-ledger", Account: "world"},
		Asset:      "USD",
	}
	destKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{Ledger: "test-ledger", Account: "users:001"},
		Asset:      "USD",
	}

	// Source (world) has nil volumes - should be created on the fly
	mockStore.EXPECT().GetVolume(sourceKey).Return(nil, dal.ErrNotFound)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())

	mockStore.EXPECT().GetVolume(destKey).Return(nil, dal.ErrNotFound)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())

	posting := &commonpb.Posting{
		Source:      "world",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test-ledger", posting, false)
	require.NoError(t, err)
}

func TestApplyPosting_InsufficientFunds(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{Ledger: "test-ledger", Account: "bank"},
		Asset:      "USD",
	}

	// Source has input=100, output=50, balance=50, but posting is 200
	sourceVol := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(100),
		OutputKnown: commonpb.NewUint256FromUint64(50),
	}

	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVol, nil)

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(200),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test-ledger", posting, false)
	require.Error(t, err)

	var insufficientFunds *ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "bank", insufficientFunds.Account)
	require.Equal(t, "USD", insufficientFunds.Asset)
}

func TestApplyPosting_BalanceNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{Ledger: "test-ledger", Account: "bank"},
		Asset:      "USD",
	}

	// Source has no input (nil InputKnown and nil InputDiff)
	sourceVol := &raftcmdpb.VolumePair{
		OutputKnown: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVol, nil)

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(100),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test-ledger", posting, false)
	require.Error(t, err)

	var balanceNotFound *ErrBalanceNotFound
	require.ErrorAs(t, err, &balanceNotFound)
	require.Equal(t, "bank", balanceNotFound.Account)
	require.Equal(t, "USD", balanceNotFound.Asset)
}

func TestApplyPosting_ForceSkipsBalanceCheck(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{Ledger: "test-ledger", Account: "bank"},
		Asset:      "USD",
	}
	destKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{Ledger: "test-ledger", Account: "users:001"},
		Asset:      "USD",
	}

	// Source has insufficient balance, but force=true skips the check
	sourceVol := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(10),
		OutputKnown: commonpb.NewUint256FromUint64(0),
	}
	destVol := &raftcmdpb.VolumePair{
		InputKnown:  commonpb.NewUint256FromUint64(0),
		OutputKnown: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVol, nil)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(destVol, nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test-ledger", posting, true)
	require.NoError(t, err)
}

func TestApplyPosting_DiffOnlyVolume(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	sourceKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{Ledger: "test-ledger", Account: "bank"},
		Asset:      "USD",
	}
	destKey := dal.VolumeKey{
		AccountKey: dal.AccountKey{Ledger: "test-ledger", Account: "users:001"},
		Asset:      "USD",
	}

	// Source has only diff values (no Known), InputDiff=1000
	sourceVol := &raftcmdpb.VolumePair{
		InputDiff:  commonpb.NewUint256FromUint64(1000),
		OutputDiff: commonpb.NewUint256FromUint64(0),
	}
	destVol := &raftcmdpb.VolumePair{
		InputDiff:  commonpb.NewUint256FromUint64(0),
		OutputDiff: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVol, nil)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(destVol, nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(100),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test-ledger", posting, false)
	require.NoError(t, err)
}
