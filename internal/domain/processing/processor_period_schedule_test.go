package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestProcessSetPeriodSchedule_ValidCron(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().SetPeriodSchedule("0 0 1 * *")

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SetPeriodSchedule{
			SetPeriodSchedule: &raftcmdpb.SetPeriodScheduleOrder{
				Cron: "0 0 1 * *",
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	scheduleLog := result.GetSetPeriodSchedule()
	require.NotNil(t, scheduleLog)
	require.Equal(t, "0 0 1 * *", scheduleLog.GetCron())
}

func TestProcessSetPeriodSchedule_InvalidCron(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SetPeriodSchedule{
			SetPeriodSchedule: &raftcmdpb.SetPeriodScheduleOrder{
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

func TestProcessDeletePeriodSchedule(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().DeletePeriodSchedule()

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_DeletePeriodSchedule{
			DeletePeriodSchedule: &raftcmdpb.DeletePeriodScheduleOrder{},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	deleteLog := result.GetDeletePeriodSchedule()
	require.NotNil(t, deleteLog)
}
