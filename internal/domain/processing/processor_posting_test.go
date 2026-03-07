package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func TestApplyPosting_WorldAccount_SkipsBalanceCheck(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	sourceKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "world"},
		Asset:      "USD",
	}
	destKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "users:001"},
		Asset:      "USD",
	}

	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(sourceKey).Return(zeroVol, nil)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())

	mockStore.EXPECT().GetVolume(destKey).Return(zeroVol, nil)
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

	sourceKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "bank"},
		Asset:      "USD",
	}

	// Source has input=100, output=50, balance=50, but posting is 200
	sourceVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(50),
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

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "bank", insufficientFunds.Account)
	require.Equal(t, "USD", insufficientFunds.Asset)
}

func TestApplyPosting_ZeroInputBalance(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	sourceKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "bank"},
		Asset:      "USD",
	}

	// Source has zero input balance, Output=0
	sourceVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVol, nil)

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(100),
		Asset:       "USD",
	}

	// Zero input means posting amount > 0 triggers ErrInsufficientFunds
	err := applyPosting(mockStore, "test-ledger", posting, false)
	require.Error(t, err)

	var insufficientFunds *domain.ErrInsufficientFunds
	require.ErrorAs(t, err, &insufficientFunds)
	require.Equal(t, "bank", insufficientFunds.Account)
	require.Equal(t, "USD", insufficientFunds.Asset)
}

func TestApplyPosting_ForceSkipsBalanceCheck(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	sourceKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "bank"},
		Asset:      "USD",
	}
	destKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "users:001"},
		Asset:      "USD",
	}

	// Source has insufficient balance, but force=true skips the check
	sourceVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(10),
		Output: commonpb.NewUint256FromUint64(0),
	}
	destVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
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

func TestApplyPosting_NotPreloaded(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	sourceKey := domain.VolumeKey{
		AccountKey: domain.AccountKey{Ledger: "test-ledger", Account: "bank"},
		Asset:      "USD",
	}

	mockStore.EXPECT().GetVolume(sourceKey).Return(nil, nil)

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(100),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, "test-ledger", posting, false)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not fully preloaded")
}
