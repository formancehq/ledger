package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestServerGetSigningConfig(t *testing.T) {
	t.Parallel()

	for _, want := range []bool{false, true} {
		impl, mockCtrl := newListHandlerHarness(t)
		mockCtrl.EXPECT().GetSigningConfig(gomock.Any()).Return(want, nil)

		resp, err := impl.GetSigningConfig(context.Background(), &servicepb.GetSigningConfigRequest{})
		require.NoError(t, err)
		require.Equal(t, want, resp.GetRequireSignatures())
	}

	t.Run("controller error", func(t *testing.T) {
		t.Parallel()

		impl, mockCtrl := newListHandlerHarness(t)
		boom := errors.New("boom")
		mockCtrl.EXPECT().GetSigningConfig(gomock.Any()).Return(false, boom)

		_, err := impl.GetSigningConfig(context.Background(), &servicepb.GetSigningConfigRequest{})
		require.ErrorIs(t, err, boom)
	})
}

func TestClientGetSigningConfig(t *testing.T) {
	t.Parallel()

	for _, want := range []bool{false, true} {
		mock := NewMockBucketServiceClient(gomock.NewController(t))
		mock.EXPECT().GetSigningConfig(gomock.Any(), gomock.Any()).Return(
			&servicepb.GetSigningConfigResponse{RequireSignatures: want}, nil,
		)

		got, err := NewLedgerGrpcClient(mock).GetSigningConfig(context.Background())
		require.NoError(t, err)
		require.Equal(t, want, got)
	}

	t.Run("transport error", func(t *testing.T) {
		t.Parallel()

		boom := errors.New("boom")
		mock := NewMockBucketServiceClient(gomock.NewController(t))
		mock.EXPECT().GetSigningConfig(gomock.Any(), gomock.Any()).Return(nil, boom)

		_, err := NewLedgerGrpcClient(mock).GetSigningConfig(context.Background())
		require.ErrorIs(t, err, boom)
	})
}
