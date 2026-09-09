package node

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/fx"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/otlplogs"
	transportpkg "github.com/formancehq/ledger/v3/internal/infra/transport"
)

// Block the dispatcher in a real peer publication, away from its stop receive.
// Channels let synctest establish quiescence without scheduler timing guesses.
type shutdownHistogram struct {
	metric.Int64Histogram

	entered chan struct{}
	release chan struct{}
}

func (h shutdownHistogram) Record(context.Context, int64, ...metric.RecordOption) {
	close(h.entered)
	<-h.release
}

func TestTransportShutdown(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"normal", "cancelAfterHookEntry", "cancelDuringCleanup", "alreadyCanceled"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				pool := transportpkg.NewConnectionPool(transportpkg.TLSPolicy{}, transportpkg.PoolConfig{})
				tr := NewTransport(logging.Testing(), pool, noop.NewMeterProvider(), 1,
					TransportConfig{Reception: []int{1, 1, 1}, Send: []int{1, 1, 1}}, "test", 1, "", "")
				peer := newTestPeerConn(t, 2, func(uint64) bool { return true })
				peer.highPriorityCh = make(chan []*raftpb.Message, 1)
				peer.stopCtx, peer.stopCancel = context.WithCancel(context.Background())
				peer.loopDone = make(chan struct{})
				peerRelease := make(chan struct{})
				go func() {
					<-peer.stopCtx.Done()
					<-peerRelease
					close(peer.loopDone)
				}()
				tr.peers[2] = peer
				publication := shutdownHistogram{entered: make(chan struct{}), release: make(chan struct{})}
				peer.sendQueueLoadHistogram[0] = publication

				// Mirror the bootstrap Fx hook: Stop is followed by an unconditional GoWait
				// join. App.Stop itself can return on cancellation while its hook continues.
				hookEntered := make(chan struct{})
				hookDone := make(chan struct{})
				workerDone := make(chan struct{})
				var hookErr error
				app := fx.New(fx.NopLogger, fx.Invoke(func(lc fx.Lifecycle) {
					var wait func()
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							wait = otlplogs.GoWait(func() {
								tr.Start(context.WithoutCancel(ctx))
								close(workerDone)
							}, logging.Testing())

							return nil
						},
						OnStop: func(ctx context.Context) error {
							close(hookEntered)
							hookErr = tr.Stop(ctx)
							wait()
							close(hookDone)

							return hookErr
						},
					})
				}))
				require.NoError(t, app.Start(context.Background()))
				tr.Send([]*raftpb.Message{{To: proto.Uint64(2), Type: raftpb.MsgHeartbeat.Enum()}})
				<-publication.entered

				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				// Fx skips hooks if the context is canceled before dispatch. Exercise that
				// boundary directly through Stop, while retaining the same worker join.
				appDone := make(chan error, 1)
				if mode == "alreadyCanceled" {
					cancel()
					go func() { appDone <- tr.Stop(ctx) }()
				} else {
					go func() { appDone <- app.Stop(ctx) }()
					<-hookEntered
				}
				synctest.Wait()
				require.True(t, tr.stopped.Load(), "Stop must have entered before cancellation")
				if mode == "cancelAfterHookEntry" {
					cancel()
					synctest.Wait()
				}
				if mode == "cancelAfterHookEntry" || mode == "alreadyCanceled" {
					require.ErrorIs(t, <-appDone, context.Canceled)
				}
				assert.False(t, shutdownComplete(workerDone), "worker must wait for cleanup")
				close(publication.release)
				synctest.Wait()
				if mode == "cancelDuringCleanup" {
					cancel()
					synctest.Wait()
					require.ErrorIs(t, <-appDone, context.Canceled)
				}
				// Cleanup must join the peer before the worker (and thus Fx hook) exits.
				assert.False(t, shutdownComplete(workerDone), "worker must wait for cleanup")
				close(peerRelease)
				synctest.Wait()

				finished := false
				select {
				case <-workerDone:
					finished = true
				default:
				}
				// Rescue the old implementation after observing the leak so BEFORE_FIX
				// fails on the assertion, without leaving goroutines behind in the bubble.
				if !finished {
					require.NoError(t, tr.Stop(context.Background()))
					synctest.Wait()
				}
				require.True(t, finished, "transport worker must finish after cancellation without a second stop signal")
				if mode != "alreadyCanceled" {
					<-hookDone
					if mode == "normal" {
						require.NoError(t, <-appDone)
						require.NoError(t, hookErr)
					} else {
						require.ErrorIs(t, hookErr, context.Canceled)
					}
				}
				assert.True(t, shutdownComplete(peer.loopDone))
				assert.True(t, peer.stopped, "peer queues must close before worker join")
				require.Len(t, peer.highPriorityCh, 1)
				<-peer.highPriorityCh // Discard the batch whose publication we held.
				for _, queue := range []chan []*raftpb.Message{peer.highPriorityCh, peer.mediumPriorityCh, peer.lowPriorityCh} {
					select {
					case _, open := <-queue:
						assert.False(t, open, "peer priority queue must be closed")
					default:
						t.Error("peer priority queue was left open")
					}
				}
				// Repeated Stop calls must share completion without closing peer queues twice.
				require.NoError(t, tr.Stop(context.Background()))
				if mode == "alreadyCanceled" {
					require.NoError(t, app.Stop(context.Background()))
				}
				require.ErrorContains(t, pool.AddPeer(3, "unused"), "connection pool is closed")
			})
		})
	}
}

func shutdownComplete(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}
