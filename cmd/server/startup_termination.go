package server

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/internal/bootstrap"
)

// serverTermination keeps Fx's startup/rollback context live while interrupting
// only learner registration. Expiry bounds outer waiting, not cleanup completion.
type serverTermination struct {
	join          *bootstrap.StartupTermination
	startup       context.Context
	cancelStartup context.CancelFunc
	finalization  context.Context
	cancelFinal   context.CancelCauseFunc
	budget        time.Duration

	mu        sync.Mutex
	requested bool
	closed    bool
	deadline  time.Time
	timer     *time.Timer
}

func newServerTermination(ctx context.Context, budget time.Duration) *serverTermination {
	startup, cancelStartup := context.WithCancel(ctx)
	finalization, cancelFinal := context.WithCancelCause(context.Background())

	return &serverTermination{
		join: bootstrap.NewStartupTermination(), startup: startup,
		cancelStartup: cancelStartup, finalization: finalization,
		cancelFinal: cancelFinal, budget: budget,
	}
}

func (s *serverTermination) interrupt() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed || s.requested {
		return
	}
	s.requested = true
	if s.budget > 0 {
		s.deadline = time.Now().Add(s.budget)
		s.timer = time.AfterFunc(s.budget, func() {
			s.cancelFinal(context.DeadlineExceeded)
			s.cancelStartup()
		})
	}
	s.join.Interrupt()
}

func (s *serverTermination) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.timer != nil {
		s.timer.Stop()
	}
	s.cancelStartup()
	s.cancelFinal(context.Canceled)
}

// finish includes deferred exporters in the signal budget, even if they ignore
// context cancellation. A timed-out worker may still be running at process exit.
func (s *serverTermination) finish(runErr error, finalize func(context.Context) error) error {
	result := make(chan error, 1)
	go func() { result <- finalize(s.finalization) }()
	var finalErr error
	select {
	case finalErr = <-result:
	case <-s.finalization.Done():
		finalErr = context.Cause(s.finalization)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	if s.timer != nil {
		s.timer.Stop()
	}
	// Timer delivery and result delivery can race; elapsed time wins even if
	// the timer's callback has not yet been scheduled.
	if !s.deadline.IsZero() && !time.Now().Before(s.deadline) {
		finalErr = errors.Join(finalErr, fmt.Errorf("terminating application within %s: %w", s.budget, context.DeadlineExceeded))
	}
	if !s.requested {
		// Preserve baseline log-only handling of exporter errors without a signal.
		return runErr
	}
	if errors.Is(runErr, bootstrap.ErrStartupTerminated) && onlyStartupTermination(runErr) {
		runErr = nil
	}

	return errors.Join(runErr, finalErr)
}

// Fx joins the failing start error with rollback failures. Never classify an
// aggregate as clean just because one branch contains intentional termination.
func onlyStartupTermination(err error) bool {
	var joined interface{ Unwrap() []error }
	if errors.As(err, &joined) {
		for _, child := range joined.Unwrap() {
			if !onlyStartupTermination(child) {
				return false
			}
		}

		return true
	}
	if child := errors.Unwrap(err); child != nil {
		return onlyStartupTermination(child)
	}

	return errors.Is(err, bootstrap.ErrStartupTerminated) || errors.Is(err, service.ErrStartFailed)
}
