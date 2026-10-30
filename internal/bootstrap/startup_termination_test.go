package bootstrap

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/fx"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

type terminationBootstrapServer struct {
	clusterbootstrappb.UnimplementedClusterBootstrapServiceServer

	join func(context.Context) (*clusterbootstrappb.JoinAsLearnerResponse, error)
}

func (s terminationBootstrapServer) JoinAsLearner(ctx context.Context, _ *clusterbootstrappb.JoinAsLearnerRequest) (*clusterbootstrappb.JoinAsLearnerResponse, error) {
	return s.join(ctx)
}

func terminationJoinConfig(t *testing.T, address string) Config {
	t.Helper()

	return Config{
		ClusterID: "termination-test",
		TLSConfig: TLSConfig{Mode: TLSModeDisabled},
		RaftConfig: node.NodeConfig{
			NodeID: 2, WalDir: t.TempDir(), AdvertiseAddr: "127.0.0.1:7777",
			Peers: []node.Peer{{ID: 1, Address: address}},
		},
	}
}

func TestStartupTerminationCancelsJoinAndPreservesRollback(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{})
	canceled := make(chan struct{})
	address := serveBootstrap(t, terminationBootstrapServer{join: func(ctx context.Context) (*clusterbootstrappb.JoinAsLearnerResponse, error) {
		close(entered)
		<-ctx.Done()
		close(canceled)

		return nil, status.FromContextError(ctx.Err()).Err()
	}})
	cfg := terminationJoinConfig(t, address)
	termination := NewStartupTermination()
	var trafficStarted atomic.Bool
	rollback := make(chan error, 1)
	dataDir := t.TempDir()
	var listenerAddress string
	app := fx.New(fx.NopLogger, fx.Invoke(func(lc fx.Lifecycle) {
		var listener net.Listener
		var store *dal.Store
		lc.Append(fx.Hook{
			OnStart: func(context.Context) error {
				var err error
				listener, err = net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					return err
				}
				listenerAddress = listener.Addr().String()
				t.Cleanup(func() {
					// Failure cleanup must also release a hook that startup did
					// not finish; rollback may already have closed the listener.
					if err := listener.Close(); err != nil && !errors.Is(err, net.ErrClosed) {
						t.Errorf("closing prior-hook listener: %v", err)
					}
				})
				store, err = dal.NewStore(dataDir, logging.Testing(), noop.Meter{}, dal.DefaultConfig())
				if err != nil {
					return err
				}
				t.Cleanup(func() { require.NoError(t, store.Close()) })

				return nil
			},
			OnStop: func(ctx context.Context) error {
				err := errors.Join(ctx.Err(), listener.Close(), store.Close())
				rollback <- err

				return err
			},
		})
		lc.Append(termination.hook(joinPreflightHook(cfg, logging.Testing())))
		lc.Append(fx.Hook{OnStart: func(context.Context) error {
			trafficStarted.Store(true)

			return nil
		}})
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- app.Start(ctx) }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("registration did not start")
	}
	termination.Interrupt()
	select {
	case err := <-result:
		require.ErrorIs(t, err, ErrStartupTerminated)
	case <-ctx.Done():
		t.Fatal("registration cancellation did not unwind Fx")
	}
	select {
	case err := <-rollback:
		require.NoError(t, err, "rollback must retain a live startup context")
	case <-ctx.Done():
		t.Fatal("Fx did not roll back its previously started hook")
	}
	select {
	case <-canceled:
	case <-ctx.Done():
		t.Fatal("outbound RPC did not observe cancellation")
	}
	require.False(t, trafficStarted.Load())
	require.False(t, wal.IsClusterJoined(cfg.RaftConfig.WalDir))

	// Prove rollback released actual resources before any process boundary or
	// fixture cleanup could release their kernel locks on its behalf.
	reopened, err := dal.NewStore(dataDir, logging.Testing(), noop.Meter{}, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, reopened.Close()) })
	rebound, err := net.Listen("tcp", listenerAddress)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rebound.Close()) })
}

func TestStartupTerminationPrelatchedSignalSkipsRegistration(t *testing.T) {
	t.Parallel()

	var requests atomic.Int32
	address := serveBootstrap(t, terminationBootstrapServer{join: func(context.Context) (*clusterbootstrappb.JoinAsLearnerResponse, error) {
		requests.Add(1)

		return &clusterbootstrappb.JoinAsLearnerResponse{}, nil
	}})
	cfg := terminationJoinConfig(t, address)
	termination := NewStartupTermination()
	termination.Interrupt()
	termination.Interrupt()
	err := termination.hook(joinPreflightHook(cfg, logging.Testing())).OnStart(context.Background())
	require.ErrorIs(t, err, ErrStartupTerminated)
	require.Zero(t, requests.Load())
	require.False(t, wal.IsClusterJoined(cfg.RaftConfig.WalDir))
}

func TestStartupTerminationPreservesRegistrationResult(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name          string
		code          codes.Code
		markerFailure bool
	}{
		{name: "success", code: codes.OK},
		{name: "already exists", code: codes.AlreadyExists},
		{name: "fatal authentication", code: codes.Unauthenticated},
		{name: "fatal peer cancellation before signal", code: codes.Canceled},
		{name: "success marker failure", code: codes.OK, markerFailure: true},
		{name: "already exists marker failure", code: codes.AlreadyExists, markerFailure: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			address := serveBootstrap(t, terminationBootstrapServer{join: func(context.Context) (*clusterbootstrappb.JoinAsLearnerResponse, error) {
				if tc.code != codes.OK {
					return nil, status.Error(tc.code, "registration result")
				}

				return &clusterbootstrappb.JoinAsLearnerResponse{}, nil
			}})
			cfg := terminationJoinConfig(t, address)
			if tc.markerFailure {
				_, err := wal.EnsureInstanceID(cfg.RaftConfig.WalDir)
				require.NoError(t, err)
				require.NoError(t, os.Mkdir(filepath.Join(cfg.RaftConfig.WalDir, wal.ClusterJoinedMarkerFile), 0o700))
			}
			termination := NewStartupTermination()
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := termination.run(ctx, func(ctx context.Context) error {
				result := tryAddLearner(ctx, cfg, cfg.TLSConfig, logging.Testing())
				// Deliver the signal after the real RPC/marker outcome is known,
				// but before the cancellation wrapper classifies its result.
				termination.Interrupt()
				<-ctx.Done() // Observe the signal before classifying the already completed result.

				return result
			})
			switch {
			case tc.markerFailure:
				require.ErrorContains(t, err, "marking cluster joined after")
			case tc.code == codes.Unauthenticated:
				var authErr *JoinAuthError
				require.ErrorAs(t, err, &authErr)
			case tc.code == codes.Canceled:
				require.Equal(t, codes.Canceled, status.Code(err))
				require.NotErrorIs(t, err, ErrStartupTerminated)
			default:
				require.ErrorIs(t, err, ErrStartupTerminated)
				require.True(t, wal.IsClusterJoined(cfg.RaftConfig.WalDir))
			}
			if tc.markerFailure || tc.code == codes.Unauthenticated {
				require.False(t, errors.Is(err, ErrStartupTerminated))
			}
		})
	}
}

func TestStartupTerminationPreservesInheritedCancellation(t *testing.T) {
	t.Parallel()

	for _, before := range []bool{true, false} {
		t.Run(map[bool]string{true: "before registration", false: "during registration and signal"}[before], func(t *testing.T) {
			t.Parallel()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if before {
				cancel()
			}
			termination := NewStartupTermination()
			err := termination.run(ctx, func(ctx context.Context) error {
				cancel()
				termination.Interrupt()

				return ctx.Err()
			})
			require.ErrorIs(t, err, context.Canceled)
			require.NotErrorIs(t, err, ErrStartupTerminated)
		})
	}
}

func TestStartupTerminationRemoteCommitWithoutResponseCanRetry(t *testing.T) {
	t.Parallel()

	committed := make(chan struct{})
	var attempts atomic.Int32
	address := serveBootstrap(t, terminationBootstrapServer{join: func(ctx context.Context) (*clusterbootstrappb.JoinAsLearnerResponse, error) {
		if attempts.Add(1) == 1 {
			close(committed)
			<-ctx.Done()

			return nil, status.FromContextError(ctx.Err()).Err()
		}

		return nil, status.Error(codes.AlreadyExists, "same member, zero progress")
	}})
	cfg := terminationJoinConfig(t, address)
	termination := NewStartupTermination()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- termination.hook(joinPreflightHook(cfg, logging.Testing())).OnStart(ctx) }()
	select {
	case <-committed:
	case <-ctx.Done():
		t.Fatal("registration did not reach remote commit")
	}
	termination.Interrupt()
	select {
	case err := <-result:
		require.ErrorIs(t, err, ErrStartupTerminated)
	case <-ctx.Done():
		t.Fatal("canceled registration did not return")
	}
	require.False(t, wal.IsClusterJoined(cfg.RaftConfig.WalDir), "lost reply must not manufacture a local marker")
	require.True(t, shouldRunJoinPreflight(cfg, logging.Testing()))
	require.NoError(t, NewStartupTermination().hook(joinPreflightHook(cfg, logging.Testing())).OnStart(ctx))
	require.EqualValues(t, 2, attempts.Load())
	marker, err := os.ReadFile(filepath.Join(cfg.RaftConfig.WalDir, wal.ClusterJoinedMarkerFile))
	require.NoError(t, err)
	require.Empty(t, marker)
	require.False(t, shouldRunJoinPreflight(cfg, logging.Testing()))
}
