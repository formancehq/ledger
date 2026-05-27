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

	sourceKey := domain.NewVolumeKey(0, "world", "USD")
	destKey := domain.NewVolumeKey(0, "users:001", "USD")

	zeroVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(sourceKey).Return(zeroVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())

	mockStore.EXPECT().GetVolume(destKey).Return(zeroVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())

	posting := &commonpb.Posting{
		Source:      "world",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, 0, posting, false, nil)
	require.NoError(t, err)
}

func TestApplyPosting_InsufficientFunds(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	sourceKey := domain.NewVolumeKey(0, "bank", "USD")

	// Source has input=100, output=50, balance=50, but posting is 200
	sourceVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(100),
		Output: commonpb.NewUint256FromUint64(50),
	}

	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVol.AsReader(), nil)

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(200),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, 0, posting, false, nil)
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

	sourceKey := domain.NewVolumeKey(0, "bank", "USD")

	// Source has zero input balance, Output=0
	sourceVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVol.AsReader(), nil)

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(100),
		Asset:       "USD",
	}

	// Zero input means posting amount > 0 triggers ErrInsufficientFunds
	err := applyPosting(mockStore, 0, posting, false, nil)
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

	sourceKey := domain.NewVolumeKey(0, "bank", "USD")
	destKey := domain.NewVolumeKey(0, "users:001", "USD")

	// Source has insufficient balance, but force=true skips the check
	sourceVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(10),
		Output: commonpb.NewUint256FromUint64(0),
	}
	destVol := &raftcmdpb.VolumePair{
		Input:  commonpb.NewUint256FromUint64(0),
		Output: commonpb.NewUint256FromUint64(0),
	}

	mockStore.EXPECT().GetVolume(sourceKey).Return(sourceVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(sourceKey, gomock.Any())
	mockStore.EXPECT().GetVolume(destKey).Return(destVol.AsReader(), nil)
	mockStore.EXPECT().PutVolume(destKey, gomock.Any())

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(500),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, 0, posting, true, nil)
	require.NoError(t, err)
}

func TestApplyPosting_NotPreloaded(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)

	sourceKey := domain.NewVolumeKey(0, "bank", "USD")

	mockStore.EXPECT().GetVolume(sourceKey).Return(nil, nil) //nolint:nilnil // test: nil volume

	posting := &commonpb.Posting{
		Source:      "bank",
		Destination: "users:001",
		Amount:      commonpb.NewUint256FromUint64(100),
		Asset:       "USD",
	}

	err := applyPosting(mockStore, 0, posting, false, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "not fully preloaded")
}
