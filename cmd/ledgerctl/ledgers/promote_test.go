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
	progress := optionalPromotionProgress(t.Context(), "mirror", read)
	require.Nil(t, progress, "missing read scope must not block the promotion confirmation")
}

func TestOptionalPromotionProgressAllowsTransientReadErrors(t *testing.T) {
	t.Parallel()
	want := errors.New("network unavailable")
	progress := optionalPromotionProgress(t.Context(), "mirror", func(context.Context, *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
		return nil, want
	})
	require.Nil(t, progress, "an unavailable preview must not block confirmation")
}

func TestOptionalPromotionProgressReturnsVisibleProgress(t *testing.T) {
	t.Parallel()
	want := &commonpb.MirrorSyncProgress{Cursor: 5, SourceLogCount: 10}
	progress := optionalPromotionProgress(t.Context(), "mirror", func(context.Context, *servicepb.GetLedgerRequest) (*commonpb.LedgerInfo, error) {
		return &commonpb.LedgerInfo{MirrorSyncProgress: want}, nil
	})
	require.Equal(t, want, progress)
}
