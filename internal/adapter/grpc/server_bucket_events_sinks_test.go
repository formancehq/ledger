package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/adapter/eventsink"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestGetEventsSinks_RedactsCredentialsAtGRPCBoundary(t *testing.T) {
	t.Parallel()

	config := &commonpb.SinkConfig{
		Name: "webhook",
		Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{
			Endpoint: "https://example.com/events",
			Secret:   "grpc-plaintext-secret",
		}},
	}
	controller := NewMockController(gomock.NewController(t))
	controller.EXPECT().GetEventsSinks(gomock.Any()).Return(
		[]*commonpb.SinkConfig{config},
		[]*commonpb.SinkStatus{{SinkName: "webhook", Cursor: 12}},
		nil,
	)
	server := &BucketServiceServerImpl{ctrl: controller}

	response, err := server.GetEventsSinks(context.Background(), &servicepb.GetEventsSinksRequest{})
	require.NoError(t, err)
	require.Len(t, response.GetSinks(), 1)
	assert.Equal(t, eventsink.RedactConfig(config).GetHttp().GetSecret(), response.GetSinks()[0].GetHttp().GetSecret())
	assert.NotEqual(t, "grpc-plaintext-secret", response.GetSinks()[0].GetHttp().GetSecret())
	assert.Equal(t, "grpc-plaintext-secret", config.GetHttp().GetSecret())
	assert.Equal(t, uint64(12), response.GetSinkStatuses()[0].GetCursor())
}
