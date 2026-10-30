package bootstrap

import (
	"context"
	"errors"

	"go.uber.org/fx"
)

// ErrStartupTerminated identifies an interrupted preflight, after which Fx must
// still roll back previously started hooks using its live startup context.
var ErrStartupTerminated = errors.New("startup interrupted by termination signal")

// StartupTermination interrupts learner registration independently of the Fx
// startup context. It is process-local state, never persisted or used by apply.
type StartupTermination struct {
	ctx       context.Context
	interrupt context.CancelCauseFunc
}

func NewStartupTermination() *StartupTermination {
	ctx, cancel := context.WithCancelCause(context.Background())

	return &StartupTermination{ctx: ctx, interrupt: cancel}
}

// Interrupt latches a supported process signal, including before preflight.
func (s *StartupTermination) Interrupt() {
	s.interrupt(ErrStartupTerminated)
}

func (s *StartupTermination) hook(hook fx.Hook) fx.Hook {
	start := hook.OnStart
	hook.OnStart = func(ctx context.Context) error {
		return s.run(ctx, start)
	}

	return hook
}

func (s *StartupTermination) run(ctx context.Context, start func(context.Context) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if s.ctx.Err() != nil {
		return ErrStartupTerminated
	}

	joinCtx, cancel := context.WithCancelCause(ctx)
	stop := context.AfterFunc(s.ctx, func() { cancel(ErrStartupTerminated) })
	defer stop()
	defer cancel(nil)
	err := start(joinCtx)
	// A signal must not hide parent cancellation, a fatal peer response, or a
	// failed joined-marker write. Successful registration writes its marker
	// before reaching this check; cancellation cannot undo a remote commit.
	if parentErr := ctx.Err(); parentErr != nil && err == nil {
		return parentErr
	}
	if ctx.Err() == nil && s.ctx.Err() != nil && (err == nil || errors.Is(err, ErrStartupTerminated)) {
		return ErrStartupTerminated
	}

	return err
}

// joinCancellation captures the cause at the retry/RPC boundary. A signal that
// arrives after the retry loop returns a fatal result must not reclassify it.
func joinCancellation(ctx context.Context) error {
	if errors.Is(context.Cause(ctx), ErrStartupTerminated) {
		return ErrStartupTerminated
	}

	return ctx.Err()
}

type startupTerminationParams struct {
	fx.In

	Termination *StartupTermination `optional:"true"`
}
