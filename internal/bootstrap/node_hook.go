package bootstrap

import (
	"context"
	"errors"
	"fmt"

	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/otlplogs"
)

// lifecycleNode is the node lifecycle owned by the bootstrap hook.
type lifecycleNode interface {
	Run(context.Context, chan struct{}) error
	Stop(context.Context) error
}

func nodeHook(node lifecycleNode, logger logging.Logger) fx.Hook {
	var cancelRun context.CancelFunc

	return fx.Hook{
		OnStart: func(ctx context.Context) error {
			ready := make(chan struct{})

			// The run context outlives startup and remains live through task
			// drain. Cancellation alone neither stops idle tasks nor joins Run.
			var runCtx context.Context
			runCtx, cancelRun = context.WithCancel(context.Background())

			runDone := make(chan struct{})
			otlplogs.Go(func() {
				defer close(runDone)
				err := node.Run(runCtx, ready)
				if err != nil {
					panic(err)
				}
			}, logger)

			select {
			case <-ctx.Done():
			case <-ready:
			}
			if err := ctx.Err(); err != nil {
				defer cancelRun()

				// Fx will not stop a hook whose OnStart failed. Own cleanup
				// here, even after the startup deadline. Node.Stop reads state
				// initialized by Run, so readiness (or completion) must happen
				// first, including when cancellation beats goroutine startup.
				select {
				case <-runDone:
					return err
				case <-ready:
				}
				stopErr := node.Stop(context.WithoutCancel(ctx))
				// A timeout is not a join. Keep dependencies owned by this
				// hook until Run exits, regardless of the Stop result.
				<-runDone
				if stopErr != nil {
					return errors.Join(err, fmt.Errorf("stopping raft cluster after cancelled startup: %w", stopErr))
				}

				return err
			}

			logger.Infof("Raft cluster started successfully")

			return nil
		},
		OnStop: func(ctx context.Context) error {
			logger.Infof("Shutting down raft cluster")

			// Do NOT cancel peer connections here. node.Stop's
			// first move is tryTransferLeadershipBeforeShutdown,
			// which needs the priority send queue of the elected
			// transferee to still be wired up so the MsgTimeoutNow
			// reaches it. Killing peer loops up-front broke the
			// transfer and forced the cluster through a full
			// election timeout on every graceful shutdown (#314).
			//
			// The transport's own fx OnStop runs AFTER this hook
			// (fx invokes OnStop in reverse registration order) and
			// already calls CancelPeerConnections inside t.Stop().
			//
			// Do NOT cancel runCtx here either. node.Run's outer
			// select watches stopChannel and tasks.err() — it does
			// not watch ctx. The tasks (applier.Run, processReadies)
			// likewise select only on their stop channel. Cancelling
			// runCtx would only propagate into the FSM calls those
			// tasks make (PrepareEntries, CommitPreparedBatch,
			// InstallSnapshot) and cause them to return
			// context.Canceled mid-batch — which surfaces as a "task
			// pool error" from Node.Run, panics the bootstrap
			// goroutine, and crashes the process mid-shutdown
			// instead of returning a clean nil (#345).
			// Cancel only after Stop returns, preserving the run
			// context through successful task/commit drain. Stop
			// publishes its shutdown request even if ctx expires
			// during transfer or before Run reaches its stop select.
			// On timeout, cancellation can interrupt context-aware
			// work, but does not join Run or terminate idle tasks;
			// Run's explicit stop path still owns their termination.
			// A timeout is not proof that infrastructure is safe to
			// close. Only successful Stop confirms the drain/join.
			defer cancelRun()

			err := node.Stop(ctx)
			if err != nil {
				return fmt.Errorf("shutting down raft cluster: %w", err)
			}

			logger.Infof("Raft cluster stopped successfully")

			return nil
		},
	}
}
