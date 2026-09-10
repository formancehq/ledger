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

	sinkConfig := &commonpb.SinkConfigInput{
		Name: "my-nats-sink",
		Type: &commonpb.SinkConfigInput_Nats{
			Nats: &commonpb.NatsSinkConfigInput{
				Url:   "nats://publisher:secret@localhost:4222",
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

	before := order.MarshalDeterministicVT(nil)
	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	addedLog := result.GetAddedEventsSink()
	require.NotNil(t, addedLog)
	require.Equal(t, "my-nats-sink", addedLog.GetConfig().GetName())
	require.Equal(t, before, order.MarshalDeterministicVT(nil))
	server := addedLog.GetConfig().GetNats().GetServers()[0]
	require.Equal(t, "localhost", server.GetAddress().GetHost())
	require.Equal(t, "publisher", server.GetUsername())
	require.Equal(t, "secret", server.GetPassword())
	server.Username = "changed"
	require.Equal(t, before, order.MarshalDeterministicVT(nil))
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
						Config: &commonpb.SinkConfigInput{
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

	cfg := &commonpb.SinkConfigInput{
		Name:      "max-sink",
		BatchSize: domain.MaxSinkBatchSize,
		Type:      &commonpb.SinkConfigInput_Http{Http: &commonpb.HttpSinkConfigInput{Endpoint: "https://sink.example/events"}},
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
						Config: &commonpb.SinkConfigInput{
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
						Name: "my-nats-sink",
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

func TestProcessAddEventsSink_InvalidConnectionDoesNotExposeInput(t *testing.T) {
	t.Parallel()
	mockStore := NewMockScope(gomock.NewController(t))
	mockStore.EXPECT().GetSinkConfig("invalid").Return(nil, nil)
	order := &raftcmdpb.AddEventsSinkOrder{Config: &commonpb.SinkConfigInput{
		Name: "invalid", Type: &commonpb.SinkConfigInput_Http{Http: &commonpb.HttpSinkConfigInput{Endpoint: "https://secret%zz@example.com"}},
	}}
	log, err := processAddEventsSink(order, &Context{Scope: mockStore})
	require.Nil(t, log)
	require.ErrorIs(t, err, errInvalidSinkConnection)
	require.NotContains(t, err.Error(), "secret")
}
