package processing

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestProcessAddEventsSink_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	sinkConfig := &commonpb.SinkConfig{
		Name:         "my-nats-sink",
		ControllerId: "cr-uid-1",
		Type: &commonpb.SinkConfig_Nats{
			Nats: &commonpb.NatsSinkConfig{
				Url:   "nats://localhost:4222",
				Topic: "ledger.events",
			},
		},
		Format: "json",
	}

	mockStore.EXPECT().GetSinkConfig("my-nats-sink").Return(nil, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{
					AddEventsSink: &raftcmdpb.AddEventsSinkOrder{
						Config: sinkConfig,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	addedLog := result.GetAddedEventsSink()
	require.NotNil(t, addedLog)
	require.Equal(t, "my-nats-sink", addedLog.GetConfig().GetName())
	require.Equal(t, "cr-uid-1", addedLog.GetConfig().GetControllerId())
}

func TestProcessAddEventsSink_BatchSizeTooLarge(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	// No mockStore expectations: validation must short-circuit before any
	// store access so the persisted state is never touched.
	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{
					AddEventsSink: &raftcmdpb.AddEventsSinkOrder{
						Config: &commonpb.SinkConfig{
							Name:      "huge-sink",
							BatchSize: domain.MaxSinkBatchSize + 1,
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var tooLarge *domain.ErrSinkBatchSizeTooLarge
	require.ErrorAs(t, err, &tooLarge)
	require.Equal(t, "huge-sink", tooLarge.Name)
	require.Equal(t, domain.MaxSinkBatchSize+1, tooLarge.BatchSize)
	require.Equal(t, domain.MaxSinkBatchSize, tooLarge.Max)
}

func TestProcessAddEventsSink_BatchSizeAtMaxAccepted(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	cfg := &commonpb.SinkConfig{
		Name:      "max-sink",
		BatchSize: domain.MaxSinkBatchSize,
	}
	mockStore.EXPECT().GetSinkConfig("max-sink").Return(nil, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{
					AddEventsSink: &raftcmdpb.AddEventsSinkOrder{
						Config: cfg,
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)
}

func TestProcessAddEventsSink_AlreadyExists(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	existingConfig := &commonpb.SinkConfig{Name: "my-nats-sink"}
	mockStore.EXPECT().GetSinkConfig("my-nats-sink").Return(existingConfig.AsReader(), nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_AddEventsSink{
					AddEventsSink: &raftcmdpb.AddEventsSinkOrder{
						Config: &commonpb.SinkConfig{
							Name: "my-nats-sink",
						},
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var sinkAlreadyExists *domain.ErrSinkAlreadyExists
	require.ErrorAs(t, err, &sinkAlreadyExists)
	require.Equal(t, "my-nats-sink", sinkAlreadyExists.Name)
}

func TestProcessRemoveEventsSink_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	existingConfig := &commonpb.SinkConfig{Name: "my-nats-sink"}
	mockStore.EXPECT().GetSinkConfig("my-nats-sink").Return(existingConfig.AsReader(), nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_RemoveEventsSink{
					RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{
						Name: "my-nats-sink",
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	removedLog := result.GetRemovedEventsSink()
	require.NotNil(t, removedLog)
	require.Equal(t, "my-nats-sink", removedLog.GetName())
}

func TestProcessRemoveEventsSink_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockScope(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().GetSinkConfig("my-nats-sink").Return(nil, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_RemoveEventsSink{
					RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{
						Name:         "my-nats-sink",
						ControllerId: "uid-1",
					},
				},
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.Error(t, err)
	require.Nil(t, result)

	var sinkNotFound *domain.ErrSinkNotFound
	require.ErrorAs(t, err, &sinkNotFound)
	require.Equal(t, "my-nats-sink", sinkNotFound.Name)
}

func TestProcessRemoveEventsSink_ControllerGuard(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name, current, requested string
		wantRemoved              bool
	}{
		{name: "matching owner", current: "uid-1", requested: "uid-1", wantRemoved: true},
		{name: "different owner", current: "uid-2", requested: "uid-1"},
		{name: "manual sink", current: "", requested: "uid-1"},
		{name: "unguarded manual removal", current: "uid-2", requested: "", wantRemoved: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			ctrl := gomock.NewController(t)
			store := NewMockScope(ctrl)
			store.EXPECT().GetSinkConfig("sink").Return((&commonpb.SinkConfig{Name: "sink", ControllerId: tc.current}).AsReader(), nil)
			processor, err := NewRequestProcessor(nil, 0)
			require.NoError(t, err)
			order := &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_RemoveEventsSink{RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{
					Name: "sink", ControllerId: tc.requested,
				}},
			}}}
			result, err := processor.ProcessOrder(order, store)
			if tc.wantRemoved {
				require.NoError(t, err)
				require.Equal(t, "sink", result.GetRemovedEventsSink().GetName())

				return
			}
			require.Nil(t, result)
			var mismatch *domain.ErrSinkControllerMismatch
			require.ErrorAs(t, err, &mismatch)
			require.Equal(t, tc.requested, mismatch.ControllerID)
		})
	}
}
