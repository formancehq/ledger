package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/snapshotpb"
)

// TestPrepareSnapshotRefusesResynchronizingDonor checks the code a requester
// relies on to move to another donor without waiting: FailedPrecondition is
// final for the fetcher, unlike the retried transient codes.
func TestPrepareSnapshotRefusesResynchronizingDonor(t *testing.T) {
	t.Parallel()

	server := NewSnapshotServiceServer(noopLogger{}, nil, 0, func(context.Context, uint64) error {
		return state.ErrStoreResynchronizing
	})

	_, err := server.PrepareSnapshot(context.Background(), &snapshotpb.PrepareSnapshotRequest{MinAppliedIndex: 7})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestPrepareSnapshotKeepsOtherWaitErrorsOpaque guards the branch above: an
// ordinary wait failure must not be reported as a donor refusal.
func TestPrepareSnapshotKeepsOtherWaitErrorsOpaque(t *testing.T) {
	t.Parallel()

	server := NewSnapshotServiceServer(noopLogger{}, nil, 0, func(context.Context, uint64) error {
		return errors.New("wait aborted")
	})

	_, err := server.PrepareSnapshot(context.Background(), &snapshotpb.PrepareSnapshotRequest{MinAppliedIndex: 7})
	require.Error(t, err)
	require.NotEqual(t, codes.FailedPrecondition, status.Code(err))
}
