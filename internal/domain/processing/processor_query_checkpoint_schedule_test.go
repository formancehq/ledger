package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func TestProcessSetQueryCheckpointSchedule_ValidCron(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().SetQueryCheckpointSchedule("0 0 1 * *")

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SetQueryCheckpointSchedule{
			SetQueryCheckpointSchedule: &raftcmdpb.SetQueryCheckpointScheduleOrder{
				Cron: "0 0 1 * *",
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	scheduleLog := result.GetSetQueryCheckpointSchedule()
	require.NotNil(t, scheduleLog)
	require.Equal(t, "0 0 1 * *", scheduleLog.GetCron())
}

func TestProcessSetQueryCheckpointSchedule_InvalidCron(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SetQueryCheckpointSchedule{
			SetQueryCheckpointSchedule: &raftcmdpb.SetQueryCheckpointScheduleOrder{
				Cron: "not-a-cron",
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var cronErr *domain.ErrInvalidCronExpression
	require.ErrorAs(t, err, &cronErr)
	require.Equal(t, "not-a-cron", cronErr.Expression)
}

func TestProcessDeleteQueryCheckpointSchedule(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().DeleteQueryCheckpointSchedule()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_DeleteQueryCheckpointSchedule{
			DeleteQueryCheckpointSchedule: &raftcmdpb.DeleteQueryCheckpointScheduleOrder{},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	deleteLog := result.GetDeleteQueryCheckpointSchedule()
	require.NotNil(t, deleteLog)
}
