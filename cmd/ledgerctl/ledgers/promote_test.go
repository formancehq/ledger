package ledgers

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestOptionalPromotionProgressAllowsWriteOnlyCaller(t *testing.T) {
	t.Parallel()
	read := func(_ context.Context, request *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
		require.Equal(t, "mirror", request.GetLedger())

		return nil, status.Error(codes.PermissionDenied, "missing ledger:ReadLedger")
	}
	progress, err := optionalPromotionProgress(t.Context(), "mirror", read)
	require.NoError(t, err)
	require.Nil(t, progress, "missing read scope must not block the promotion confirmation")
}

func TestOptionalPromotionProgressPreservesReadErrors(t *testing.T) {
	t.Parallel()
	want := errors.New("network unavailable")
	progress, err := optionalPromotionProgress(t.Context(), "mirror", func(context.Context, *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
		return nil, want
	})
	require.Nil(t, progress)
	require.ErrorIs(t, err, want)
}

func TestOptionalPromotionProgressReturnsVisibleProgress(t *testing.T) {
	t.Parallel()
	want := &commonpb.MirrorSyncProgress{Cursor: 5, SourceLogCount: 10}
	progress, err := optionalPromotionProgress(t.Context(), "mirror", func(context.Context, *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
		return &commonpb.LedgerInfo{MirrorSyncProgress: want}, nil
	})
	require.NoError(t, err)
	require.Equal(t, want, progress)
}
