package processing

import (
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestProcessAddEventsSink_Success(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	sinkConfig := &commonpb.SinkConfig{
		Name: "my-nats-sink",
		Type: &commonpb.SinkConfig_Nats{
			Nats: &commonpb.NatsSinkConfig{
				Url:   "nats://localhost:4222",
				Topic: "ledger.events",
			},
		},
		Format: "json",
	}

	mockStore.EXPECT().GetSinkConfig("my-nats-sink").Return(nil, nil)
	mockStore.EXPECT().AddSinkConfig(sinkConfig)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_AddEventsSink{
			AddEventsSink: &raftcmdpb.AddEventsSinkOrder{
				Config: sinkConfig,
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	addedLog := result.GetAddedEventsSink()
	require.NotNil(t, addedLog)
	require.Equal(t, "my-nats-sink", addedLog.Config.Name)
}

func TestProcessAddEventsSink_AlreadyExists(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	existingConfig := &commonpb.SinkConfig{Name: "my-nats-sink"}
	mockStore.EXPECT().GetSinkConfig("my-nats-sink").Return(existingConfig, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_AddEventsSink{
			AddEventsSink: &raftcmdpb.AddEventsSinkOrder{
				Config: &commonpb.SinkConfig{
					Name: "my-nats-sink",
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

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	existingConfig := &commonpb.SinkConfig{Name: "my-nats-sink"}
	mockStore.EXPECT().GetSinkConfig("my-nats-sink").Return(existingConfig, nil)
	mockStore.EXPECT().RemoveSinkConfig("my-nats-sink")

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_RemoveEventsSink{
			RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{
				Name: "my-nats-sink",
			},
		},
	}

	result, err := processor.ProcessOrder(order, mockStore)
	require.NoError(t, err)
	require.NotNil(t, result)

	removedLog := result.GetRemovedEventsSink()
	require.NotNil(t, removedLog)
	require.Equal(t, "my-nats-sink", removedLog.Name)
}

func TestProcessRemoveEventsSink_NotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockStore := NewMockInMemoryStore(ctrl)
	processor, err := NewRequestProcessor(nil, 0)
	require.NoError(t, err)

	mockStore.EXPECT().GetSinkConfig("my-nats-sink").Return(nil, nil)

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_RemoveEventsSink{
			RemoveEventsSink: &raftcmdpb.RemoveEventsSinkOrder{
				Name: "my-nats-sink",
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
