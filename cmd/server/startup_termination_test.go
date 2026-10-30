package server

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/internal/bootstrap"
)

func TestServerTerminationBudget(t *testing.T) {
	t.Parallel()
	for _, budget := range []time.Duration{0, 2 * time.Second} {
		t.Run(budget.String(), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				termination := newServerTermination(t.Context(), budget)
				defer termination.close()
				termination.interrupt()
				require.NoError(t, termination.startup.Err(), "signal must leave the Fx rollback context live")
				if budget == 0 {
					time.Sleep(time.Hour)
					synctest.Wait()
					require.NoError(t, termination.startup.Err(), "zero retains unlimited startup cleanup")
					require.NoError(t, termination.finalization.Err())
					require.NoError(t, termination.finish(bootstrap.ErrStartupTerminated, func(context.Context) error { return nil }))

					return
				}
				time.Sleep(budget)
				synctest.Wait()
				require.ErrorIs(t, termination.startup.Err(), context.Canceled)
				require.ErrorIs(t, context.Cause(termination.finalization), context.DeadlineExceeded)
				require.ErrorIs(t, termination.finish(bootstrap.ErrStartupTerminated, func(context.Context) error { return nil }), context.DeadlineExceeded)
			})
		})
	}
}

func TestServerTerminationPreservesErrors(t *testing.T) {
	t.Parallel()
	startupFailure := errors.New("fatal startup failure")
	finalizationFailure := errors.New("logger shutdown failure")
	for _, test := range []struct {
		name        string
		signal      bool
		runErr      error
		finalErr    error
		expectedErr error
	}{
		{name: "clean interrupted startup", signal: true, runErr: fmt.Errorf("start: %w", errors.Join(service.ErrStartFailed, bootstrap.ErrStartupTerminated))},
		{name: "fatal startup during signal", signal: true, runErr: startupFailure, expectedErr: startupFailure},
		{name: "inherited cancellation during signal", signal: true, runErr: context.Canceled, expectedErr: context.Canceled},
		{name: "signal logger failure", signal: true, runErr: bootstrap.ErrStartupTerminated, finalErr: finalizationFailure, expectedErr: finalizationFailure},
		{name: "no signal logger failure remains log only", finalErr: finalizationFailure},
		{name: "no signal startup error retained", runErr: startupFailure, finalErr: finalizationFailure, expectedErr: startupFailure},
		{name: "no signal does not suppress termination sentinel", runErr: bootstrap.ErrStartupTerminated, expectedErr: bootstrap.ErrStartupTerminated},
	} {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				termination := newServerTermination(t.Context(), time.Minute)
				defer termination.close()
				if test.signal {
					termination.interrupt()
				}
				contexts := make(chan context.Context, 1)
				err := termination.finish(test.runErr, func(ctx context.Context) error {
					contexts <- ctx

					return test.finalErr
				})
				finalizationContext := <-contexts
				require.NotNil(t, finalizationContext)
				require.NoError(t, finalizationContext.Err(), "finalization starts with the remaining live budget")
				if test.expectedErr == nil {
					require.NoError(t, err)
				} else {
					require.ErrorIs(t, err, test.expectedErr)
				}
			})
		})
	}
}

func TestServerTerminationRetainsFxRollbackFailure(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		termination := newServerTermination(t.Context(), time.Minute)
		defer termination.close()
		rollbackFailure := errors.New("store close failure")
		var rollbackContextErr error
		app := fx.New(fx.NopLogger, fx.Invoke(func(lifecycle fx.Lifecycle) {
			lifecycle.Append(fx.Hook{OnStart: func(context.Context) error { return nil }, OnStop: func(ctx context.Context) error {
				rollbackContextErr = ctx.Err()

				return rollbackFailure
			}})
			lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
				termination.interrupt()

				return bootstrap.ErrStartupTerminated
			}})
		}))
		startErr := app.Start(termination.startup)
		require.ErrorIs(t, startErr, bootstrap.ErrStartupTerminated)
		require.ErrorIs(t, startErr, rollbackFailure)
		require.NoError(t, rollbackContextErr, "rollback must receive a live startup context")
		err := termination.finish(fmt.Errorf("%w: %w", service.ErrStartFailed, startErr), func(context.Context) error { return nil })
		require.ErrorIs(t, err, rollbackFailure, "intentional interruption must not hide failed rollback")
	})
}

func TestServerTerminationBoundsNonJoinStartupHook(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		termination := newServerTermination(t.Context(), time.Second)
		defer termination.close()
		entered, release, hookDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
		app := fx.New(fx.NopLogger, fx.Invoke(func(lifecycle fx.Lifecycle) {
			lifecycle.Append(fx.Hook{OnStart: func(context.Context) error {
				close(entered)
				<-release // Deliberately ignore cancellation to exercise Fx's outer bound.
				close(hookDone)

				return nil
			}})
		}))
		started := make(chan error, 1)
		go func() { started <- app.Start(termination.startup) }()
		<-entered
		defer func() {
			close(release)
			<-hookDone
		}()
		termination.interrupt()
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case err := <-started:
			require.ErrorIs(t, err, context.Canceled)
		default:
			t.Error("Fx Start must return when the signal budget expires")
		}
		require.ErrorIs(t, termination.finish(context.Canceled, func(context.Context) error { return nil }), context.DeadlineExceeded)
	})
}

func TestServerTerminationBoundsIgnoringFinalizer(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		termination := newServerTermination(t.Context(), time.Second)
		defer termination.close()
		termination.interrupt()
		entered, release, finalized := make(chan struct{}), make(chan struct{}), make(chan struct{})
		finished := make(chan error, 1)
		go func() {
			finished <- termination.finish(bootstrap.ErrStartupTerminated, func(context.Context) error {
				close(entered)
				<-release // Model a logger that ignores its cancellation context.
				close(finalized)

				return nil
			})
		}()
		<-entered
		defer func() {
			close(release)
			<-finalized
		}()
		time.Sleep(time.Second)
		synctest.Wait()
		select {
		case err := <-finished:
			require.ErrorIs(t, err, context.DeadlineExceeded)
		default:
			t.Error("waiting for the finalizer must respect the signal budget")
		}
	})
}

func TestServerTerminationRejectsFinalizerSuccessAtDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		termination := newServerTermination(t.Context(), time.Second)
		defer termination.close()
		termination.interrupt()
		finalized := make(chan struct{})
		err := termination.finish(bootstrap.ErrStartupTerminated, func(context.Context) error {
			<-time.After(time.Second)
			close(finalized)

			return nil
		})
		require.ErrorIs(t, err, context.DeadlineExceeded, "deadline wins when successful completion is simultaneously ready")
		<-finalized
	})
}

func TestServerTerminationDuplicateAndClosedInterrupts(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		termination := newServerTermination(t.Context(), 2*time.Second)
		termination.interrupt()
		time.Sleep(time.Second)
		termination.interrupt()
		time.Sleep(time.Second)
		synctest.Wait()
		require.ErrorIs(t, context.Cause(termination.finalization), context.DeadlineExceeded, "repeated signals must not extend the budget")
		termination.close()
		termination.close()
		termination.interrupt()

		completed := newServerTermination(t.Context(), time.Second)
		defer completed.close()
		completed.interrupt()
		require.NoError(t, completed.finish(bootstrap.ErrStartupTerminated, func(context.Context) error { return nil }))
		completed.interrupt()
		time.Sleep(time.Hour)
		synctest.Wait()
		require.NoError(t, completed.startup.Err(), "successful completion must disarm the startup timer")

		closed := newServerTermination(t.Context(), time.Second)
		closed.close()
		closed.interrupt()
		time.Sleep(2 * time.Second)
		synctest.Wait()
		require.False(t, closed.requested, "a late signal must not reopen completed termination")
		require.ErrorIs(t, context.Cause(closed.finalization), context.Canceled)
	})
}

func TestServerTerminationConcurrentSignalAndCloseAtDeadline(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		termination := newServerTermination(t.Context(), time.Second)
		termination.interrupt()
		done := make(chan struct{}, 2)
		go func() {
			<-time.After(time.Second)
			termination.close()
			done <- struct{}{}
		}()
		go func() {
			<-time.After(time.Second)
			termination.interrupt()
			done <- struct{}{}
		}()
		<-done
		<-done
		synctest.Wait()
		require.ErrorIs(t, termination.startup.Err(), context.Canceled)
		require.Error(t, termination.finalization.Err())
		require.True(t, termination.closed, "a concurrent signal must not reopen completed termination")
		termination.close()
	})
}
