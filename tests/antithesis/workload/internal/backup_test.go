package internal

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// The four driver stages share this execution path. The proposal seam commits
// Start before losing its response: busy comes from real durable backup state,
// not a preprogrammed sequence of RPC errors. Orchestrator recovery tests cover
// the real cleanup executor and uploaded bytes independently of this client seam.
func TestRetryBackup_CommittedStartLostResponse(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		operation string
		kind      raftcmdpb.BackupKind
	}{
		{"Backup", raftcmdpb.BackupKind_BACKUP_KIND_FULL},
		{"Backup (pre-incremental)", raftcmdpb.BackupKind_BACKUP_KIND_FULL},
		{"IncrementalBackup", raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL},
		{"second IncrementalBackup", raftcmdpb.BackupKind_BACKUP_KIND_INCREMENTAL},
	} {
		t.Run(tc.operation, func(t *testing.T) {
			t.Parallel()
			store, err := dal.NewStore(t.TempDir(), logging.Testing(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, store.Close()) })
			jobs := state.NewBackupJobsState()
			dst := &raftcmdpb.BackupDestination{BucketId: "retry", Target: &raftcmdpb.BackupDestination_S3{
				S3: &raftcmdpb.S3BackupTarget{Bucket: "backups", Endpoint: "http://minio:9000"},
			}}

			synctest.Test(t, func(t *testing.T) {
				started := time.Now()
				attempts, cleanupCount := 0, 0
				var result uint64
				invoke := func(_ context.Context, _ string, _, _ any, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
					attempts++
					// Deliver the orphan cleanup only after its real 30s cadence.
					// No wall-clock sleep or early unlock is used to make retries pass.
					if cleanupCount == 0 && time.Since(started) >= 30*time.Second {
						batch := store.OpenWriteSession()
						_, err := jobs.Fail(batch, 100, &raftcmdpb.BackupOrderFail{JobId: 1, Message: "orphan executor"})
						require.NoError(t, err)
						require.NoError(t, batch.Commit())
						cleanupCount++
						t.Logf("cleanup committed after %s", time.Since(started))
					}
					batch := store.OpenWriteSession()
					job, _, err := jobs.Start(batch, uint64(attempts), tc.kind, &raftcmdpb.BackupOrderStart{
						JobId: uint64(attempts), Destination: dst, ExecutorNodeId: 1,
					})
					if err != nil {
						require.NoError(t, batch.Cancel())
						require.ErrorIs(t, err, state.ErrBackupInProgress)
						t.Logf("attempt %d at %s: %v", attempts, time.Since(started), err)
						return status.Error(codes.FailedPrecondition, fmt.Sprintf("propose backup start: %v", err))
					}
					require.NoError(t, batch.Commit())
					if attempts == 1 {
						// Reload from disk before replying: the lost reply follows a
						// committed Start, not an uncommitted in-memory mutation.
						jobs.Reset()
						reader, err := store.NewDirectReadHandle()
						require.NoError(t, err)
						require.NoError(t, jobs.RestoreFromStore(reader))
						require.NoError(t, reader.Close())
						require.Equal(t, uint64(1), jobs.ActiveByDestination(dst).GetJobId())
						t.Log("Start committed and reloaded; apply acknowledgment lost")
						return status.Error(codes.Unavailable, "waiting for FSM apply: context canceled")
					}
					batch = store.OpenWriteSession()
					_, err = jobs.Complete(batch, 101, &raftcmdpb.BackupOrderComplete{JobId: job.GetJobId()})
					require.NoError(t, err)
					require.NoError(t, batch.Commit())
					result = job.GetJobId()
					return nil
				}
				// This is the same automatic transient retry used by NewGRPCConn.
				automaticRetry := retryUnaryInterceptor(retryMaxAttempts)
				got, err := RetryBackup(t.Context(), tc.operation, func(ctx context.Context) (uint64, error) {
					err := automaticRetry(ctx, tc.operation, nil, nil, nil, invoke)
					return result, err
				})
				require.NoError(t, err, "%s must recover from the committed ambiguous attempt, not report unexpected busy", tc.operation)
				require.Equal(t, 11, attempts)
				require.Equal(t, uint64(11), got)
				require.Equal(t, 1, cleanupCount)
				require.Empty(t, jobs.Snapshot())
				t.Logf("recovered at %s after %d RPC attempts; final job=%d; active slots=0", time.Since(started), attempts, got)
			})
		})
	}
}

func TestRetryBackup_OnlyDestinationBusyIsRetried(t *testing.T) {
	t.Parallel()
	busy := state.ErrBackupInProgress.Error()
	for _, tc := range []struct {
		name string
		err  error
	}{
		{"job ID collision", status.Error(codes.FailedPrecondition, state.ErrBackupJobIDCollision.Error())},
		{"missing checkpoint", status.Error(codes.FailedPrecondition, "full checkpoint required")},
		{"other precondition", status.Error(codes.FailedPrecondition, "restore is not ready")},
		{"wrong code", status.Error(codes.Unknown, busy)},
		{"different suffix", status.Error(codes.FailedPrecondition, busy+" but storage is corrupted")},
		{"plain text", errors.New(busy)},
		{"server error", status.Error(codes.Internal, "invariant violation")},
		{"remote canceled", status.Error(codes.Canceled, "client connection is closing")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			attempts := 0
			_, err := RetryBackup(t.Context(), "Backup", func(context.Context) (int, error) {
				attempts++
				return 0, tc.err
			})
			require.Same(t, tc.err, err, "unrelated errors retain their identity")
			require.Equal(t, 1, attempts)
			require.False(t, IsBackupInProgress(err))
		})
	}
}

func TestRetryBackup_BusyBudget(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		start := time.Now()
		attempts := 0
		busy := status.Error(codes.FailedPrecondition, state.ErrBackupInProgress.Error())
		got, err := RetryBackup(t.Context(), "Backup", func(context.Context) (int, error) {
			attempts++
			return 0, busy
		})
		require.Zero(t, got)
		require.ErrorIs(t, err, busy)
		require.Contains(t, err.Error(), "context deadline exceeded")
		require.True(t, IsBackupInProgress(err), "budget exhaustion retains the exact last failure")
		require.Equal(t, 27, attempts)
		require.Equal(t, 2*time.Minute, time.Since(start))
	})
}

func TestRetryBackup_UnexpectedErrorAfterBusy(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		attempts := 0
		serverError := status.Error(codes.Internal, "invariant violation")
		_, err := RetryBackup(t.Context(), "Backup", func(context.Context) (int, error) {
			attempts++
			if attempts == 1 {
				return 0, status.Error(codes.FailedPrecondition, state.ErrBackupInProgress.Error())
			}
			return 0, serverError
		})
		require.Same(t, serverError, err)
		require.Equal(t, 2, attempts)
	})
}

func TestRetryBackup_Cancellation(t *testing.T) {
	t.Parallel()
	for _, alreadyCanceled := range []bool{true, false} {
		t.Run(fmt.Sprintf("before-call-%t", alreadyCanceled), func(t *testing.T) {
			t.Parallel()
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if alreadyCanceled {
				cancel()
			}
			attempts := 0
			busy := status.Error(codes.FailedPrecondition, state.ErrBackupInProgress.Error())
			_, err := RetryBackup(ctx, "Backup", func(context.Context) (int, error) {
				attempts++
				cancel()
				return 0, busy
			})
			if alreadyCanceled {
				require.ErrorIs(t, err, context.Canceled)
				require.Zero(t, attempts)
			} else {
				require.ErrorIs(t, err, busy)
				require.Contains(t, err.Error(), "context canceled")
				require.Equal(t, 1, attempts)
			}
		})
	}
}

func TestRetryBackup_DoesNotShortenAdmittedBackup(t *testing.T) {
	t.Parallel()
	for _, initiallyBusy := range []bool{false, true} {
		t.Run(fmt.Sprintf("initially-busy-%t", initiallyBusy), func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				attempts := 0
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Minute)
				defer cancel()
				got, err := RetryBackup(ctx, "Backup", func(callCtx context.Context) (int, error) {
					attempts++
					if initiallyBusy && attempts == 1 {
						return 0, status.Error(codes.FailedPrecondition, state.ErrBackupInProgress.Error())
					}
					// Successful uploads may take longer than the busy retry
					// window. The caller's existing RPC budget still governs them.
					timer := time.NewTimer(3 * time.Minute)
					defer timer.Stop()
					select {
					case <-callCtx.Done():
						return 0, callCtx.Err()
					case <-timer.C:
						return 42, nil
					}
				})
				require.NoError(t, err)
				require.Equal(t, 42, got)
				if initiallyBusy {
					require.Equal(t, 2, attempts)
				} else {
					require.Equal(t, 1, attempts)
				}
			})
		})
	}
}
