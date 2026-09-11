package bootstrap

import (
	"context"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"go.uber.org/fx/fxevent"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// controlledNode exposes lifecycle barriers, independent of scheduler timing.
type controlledNode struct {
	run  func(context.Context, chan struct{}) error
	stop func(context.Context) error
}

func (n controlledNode) Run(ctx context.Context, ready chan struct{}) error {
	return n.run(ctx, ready)
}

func (n controlledNode) Stop(ctx context.Context) error { return n.stop(ctx) }

func TestNodeHookCancelledStartupJoinsRun(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		initialize := make(chan struct{})
		terminate := make(chan struct{})
		stopped := make(chan struct{})
		exited := make(chan struct{})
		runContexts := make(chan context.Context, 1)
		n := controlledNode{
			run: func(ctx context.Context, ready chan struct{}) error {
				runContexts <- ctx
				<-initialize
				close(ready)
				<-terminate
				close(exited)

				return nil
			},
			stop: func(ctx context.Context) error {
				assert.NoError(t, ctx.Err(), "cleanup must survive startup cancellation")
				close(stopped)

				return nil
			},
		}
		hook := nodeHook(n, logging.Testing())
		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() { result <- hook.OnStart(ctx) }()
		runCtx := <-runContexts
		synctest.Wait()
		cancel()
		synctest.Wait()
		// Record violations without aborting: the unfixed hook still needs its
		// blocked Run released before the synctest bubble can finish.
		select {
		case <-result:
			t.Error("cancelled startup returned while Run was still initializing")
		default:
		}
		assert.NoError(t, runCtx.Err(), "startup cleanup must preserve the run context")
		close(initialize)
		synctest.Wait()
		select {
		case <-stopped:
		default:
			t.Error("cancelled startup did not request node shutdown")
		}
		select {
		case <-result:
			t.Error("cancelled startup returned before Run exited")
		default:
		}
		close(terminate)
		synctest.Wait()
		select {
		case err := <-result:
			require.ErrorIs(t, err, context.Canceled)
		default:
			t.Error("startup did not return after Run exited")
		}
		<-exited
		require.ErrorIs(t, runCtx.Err(), context.Canceled)
	})
}

func TestNodeHookReadyAndCancelledStartupJoinsRun(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		stopped := make(chan struct{})
		var exited bool
		n := controlledNode{
			run: func(ctx context.Context, ready chan struct{}) error {
				// Cancellation is visible when readiness is published: choosing the
				// ready branch must not turn cancelled startup into success.
				cancel()
				close(ready)
				<-stopped
				assert.NoError(t, ctx.Err(), "run context must survive task drain")
				exited = true

				return nil
			},
			stop: func(ctx context.Context) error {
				assert.NoError(t, ctx.Err())
				close(stopped)

				return nil
			},
		}
		require.ErrorIs(t, nodeHook(n, logging.Testing()).OnStart(ctx), context.Canceled)
		require.True(t, exited, "the failed hook must join even if readiness also wins")
	})
}

func TestNodeHookSuccessfulStartupPreservesRunContextThroughStop(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		stopEntered := make(chan struct{})
		terminate := make(chan struct{})
		exited := make(chan struct{})
		runContexts := make(chan context.Context, 1)
		n := controlledNode{
			run: func(ctx context.Context, ready chan struct{}) error {
				runContexts <- ctx
				close(ready)
				<-terminate
				close(exited)

				return nil
			},
			stop: func(ctx context.Context) error {
				assert.NoError(t, ctx.Err())
				close(stopEntered)
				<-exited

				return nil
			},
		}
		hook := nodeHook(n, logging.Testing())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		require.NoError(t, hook.OnStart(ctx))
		runCtx := <-runContexts
		cancel()
		synctest.Wait()
		require.NoError(t, runCtx.Err(), "successful startup owns an independent run context")
		result := make(chan error, 1)
		go func() { result <- hook.OnStop(context.Background()) }()
		<-stopEntered
		synctest.Wait()
		require.NoError(t, runCtx.Err(), "normal stop must preserve pending commit drain")
		close(terminate)
		require.NoError(t, <-result)
		require.ErrorIs(t, runCtx.Err(), context.Canceled)
	})
}

// lifecycleEvents observes real Fx rollback without assuming that an expired
// rollback context executes dependency hooks.
type lifecycleEvents struct {
	rollback chan struct{}
}

func (l lifecycleEvents) LogEvent(event fxevent.Event) {
	if _, ok := event.(*fxevent.RollingBack); ok {
		close(l.rollback)
	}
}

func TestNodeHookCancelledStartupOwnsCleanupBeforeFxRollback(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		initialize := make(chan struct{})
		terminate := make(chan struct{})
		stopped := make(chan struct{})
		exited := make(chan struct{})
		hookReturned := make(chan struct{})
		rollback := make(chan struct{})
		dependencyClosed := false
		n := controlledNode{
			run: func(_ context.Context, ready chan struct{}) error {
				<-initialize
				close(ready)
				<-terminate
				assert.False(t, dependencyClosed, "dependency must remain available until Run exits")
				close(exited)

				return nil
			},
			stop: func(ctx context.Context) error {
				assert.NoError(t, ctx.Err())
				close(stopped)

				return nil
			},
		}
		dependency := fx.Hook{
			OnStart: func(context.Context) error { return nil },
			OnStop: func(context.Context) error {
				select {
				case <-exited:
				default:
					t.Error("dependency closed before Run exited")
				}
				dependencyClosed = true

				return nil
			},
		}
		app := fx.New(
			fx.WithLogger(func() fxevent.Logger { return lifecycleEvents{rollback: rollback} }),
			fx.Invoke(func(lc fx.Lifecycle) {
				lc.Append(dependency)
				hook := nodeHook(n, logging.Testing())
				lc.Append(fx.Hook{
					OnStart: func(ctx context.Context) error {
						defer close(hookReturned)

						return hook.OnStart(ctx)
					},
					OnStop: func(ctx context.Context) error {
						t.Error("Fx must not stop a hook whose OnStart failed")

						return hook.OnStop(ctx)
					},
				})
			}),
		)
		require.NoError(t, app.Err())
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		result := make(chan error, 1)
		go func() { result <- app.Start(ctx) }()
		synctest.Wait()
		cancel()
		synctest.Wait()
		// Fx may return the startup deadline while its hook still owns cleanup.
		require.ErrorIs(t, <-result, context.Canceled)
		select {
		case <-rollback:
			t.Error("Fx rollback began before failed-start cleanup completed")
		default:
		}
		close(initialize)
		synctest.Wait()
		select {
		case <-stopped:
		default:
			t.Error("failed startup never requested Stop")
		}
		select {
		case <-hookReturned:
			t.Error("failed hook returned before Run completed")
		default:
		}
		close(terminate)
		synctest.Wait()
		<-hookReturned
		<-rollback
		require.False(t, dependencyClosed, "Fx skips stop hooks when rollback context is expired")
		// Controlled cleanup after the failed hook joined Run. Fx's expired
		// rollback did not close this resource for us.
		require.NoError(t, dependency.OnStop(context.Background()))
		require.True(t, dependencyClosed)
	})
}
