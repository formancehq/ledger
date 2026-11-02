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

type testSnapshotDonor struct {
	waitErr            error
	lastPersistedIndex uint64
	resynchronizing    bool
}

func (d testSnapshotDonor) WaitForCheckpointable(context.Context, uint64) error {
	return d.waitErr
}

func (d testSnapshotDonor) SnapshotReadiness() (uint64, bool) {
	return d.lastPersistedIndex, d.resynchronizing
}

// TestPrepareSnapshotRefusesResynchronizingDonor checks the code a requester
// relies on to move to another donor without waiting: FailedPrecondition is
// final for the fetcher, unlike the retried transient codes.
func TestPrepareSnapshotRefusesResynchronizingDonor(t *testing.T) {
	t.Parallel()

	server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{waitErr: state.ErrStoreResynchronizing})

	_, err := server.PrepareSnapshot(context.Background(), &snapshotpb.PrepareSnapshotRequest{MinAppliedIndex: 7})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestPrepareSnapshotRefusesResynchronizingDonorAtZeroIndex(t *testing.T) {
	t.Parallel()

	server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{waitErr: state.ErrStoreResynchronizing})

	_, err := server.PrepareSnapshot(context.Background(), &snapshotpb.PrepareSnapshotRequest{})
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

// TestPrepareSnapshotKeepsOtherWaitErrorsOpaque guards the branch above: an
// ordinary wait failure must not be reported as a donor refusal.
func TestPrepareSnapshotKeepsOtherWaitErrorsOpaque(t *testing.T) {
	t.Parallel()

	server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{waitErr: errors.New("wait aborted")})

	_, err := server.PrepareSnapshot(context.Background(), &snapshotpb.PrepareSnapshotRequest{MinAppliedIndex: 7})
	require.Error(t, err)
	require.NotEqual(t, codes.FailedPrecondition, status.Code(err))
}

func TestCheckSnapshotReadinessDoesNotCreateCheckpoint(t *testing.T) {
	t.Parallel()

	server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{lastPersistedIndex: 42})

	resp, err := server.CheckSnapshotReadiness(context.Background(), &snapshotpb.CheckSnapshotReadinessRequest{
		MinAppliedIndex: 41,
	})
	require.NoError(t, err)
	require.True(t, resp.GetReady())
	require.Equal(t, uint64(42), resp.GetLastPersistedIndex())
	require.False(t, resp.GetResynchronizing())
}

func TestCheckSnapshotReadinessRejectsStaleOrResynchronizingDonor(t *testing.T) {
	t.Parallel()

	t.Run("stale", func(t *testing.T) {
		server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{lastPersistedIndex: 40})

		resp, err := server.CheckSnapshotReadiness(context.Background(), &snapshotpb.CheckSnapshotReadinessRequest{
			MinAppliedIndex: 41,
		})
		require.NoError(t, err)
		require.False(t, resp.GetReady())
		require.Equal(t, uint64(40), resp.GetLastPersistedIndex())
	})

	t.Run("resynchronizing", func(t *testing.T) {
		server := NewSnapshotServiceServer(noopLogger{}, nil, 0, testSnapshotDonor{lastPersistedIndex: 42, resynchronizing: true})

		resp, err := server.CheckSnapshotReadiness(context.Background(), &snapshotpb.CheckSnapshotReadinessRequest{
			MinAppliedIndex: 41,
		})
		require.NoError(t, err)
		require.False(t, resp.GetReady())
		require.True(t, resp.GetResynchronizing())
	})
}
