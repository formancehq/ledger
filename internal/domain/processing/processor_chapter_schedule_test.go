package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestProcessSetChapterSchedule_ValidCron(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0, false)
	require.NoError(t, err)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_SetChapterSchedule{
					SetChapterSchedule: &raftcmdpb.SetChapterScheduleOrder{
						Cron: "0 0 1 * *",
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	scheduleLog := result.GetSetChapterSchedule()
	require.NotNil(t, scheduleLog)
	require.Equal(t, "0 0 1 * *", scheduleLog.GetCron())
}

func TestProcessSetChapterSchedule_InvalidCron(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0, false)
	require.NoError(t, err)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_SetChapterSchedule{
					SetChapterSchedule: &raftcmdpb.SetChapterScheduleOrder{
						Cron: "not-a-cron",
					},
				},
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

func TestProcessDeleteChapterSchedule(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0, false)
	require.NoError(t, err)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_DeleteChapterSchedule{
					DeleteChapterSchedule: &raftcmdpb.DeleteChapterScheduleOrder{},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	deleteLog := result.GetDeleteChapterSchedule()
	require.NotNil(t, deleteLog)
}
