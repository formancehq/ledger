package processing

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestProcessSetMaintenanceMode_Enable(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().SetMaintenanceMode(true)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SetMaintenanceMode{
			SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{
				Enabled: true,
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	mmLog := result.GetSetMaintenanceMode()
	require.NotNil(t, mmLog)
	require.True(t, mmLog.Enabled)
}

func TestProcessSetMaintenanceMode_Disable(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().SetMaintenanceMode(false)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SetMaintenanceMode{
			SetMaintenanceMode: &raftcmdpb.SetMaintenanceModeOrder{
				Enabled: false,
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	mmLog := result.GetSetMaintenanceMode()
	require.NotNil(t, mmLog)
	require.False(t, mmLog.Enabled)
}
