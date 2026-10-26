package generation

import (
	"errors"
	"fmt"

	"hegel.dev/go/hegel"
)

// RunOnce runs one cloud workload episode. fn runs at most once, even when
// Hegel retries or performs a final replay. Errors include native engine
// failures, callback panics, and runs that never execute fn to completion.
// Local property tests should use hegel.Test and NewSource instead, so Hegel
// can replay and shrink their pure callbacks normally.
func RunOnce(fn func(*Source)) (err error) {
	defer func() {
		if value := recover(); value != nil {
			err = errors.Join(err, panicError(value))
		}
	}()
	if fn == nil {
		return errors.New("generation: nil callback")
	}

	called := false
	var callbackErr error
	err = hegel.Run(func(tc hegel.TestCase) {
		// WithTestCases(1) and PhaseGenerate do not disable every engine
		// callback, including final failure replay. Guard before user effects.
		if called {
			return
		}
		called = true
		completed := false
		defer func() {
			if !completed {
				value := recover()
				callbackErr = panicError(value)
				// Let Hegel record its native abort/failure as well. Keep our
				// error because a guarded replay cannot reproduce user effects.
				panic(value)
			}
		}()
		fn(NewSource(tc))
		completed = true
	},
		hegel.WithProfile("workload"),
		hegel.WithBackend(hegel.BackendURandom),
		hegel.WithDatabase(""),
		hegel.WithTestCases(1),
		hegel.WithPhases(hegel.PhaseGenerate),
		// Episodes perform real RPCs and may wait for a faulted cluster.
		// That latency is expected; all other health checks remain enabled.
		hegel.SuppressHealthCheck(hegel.TooSlow),
	)
	if !called {
		err = errors.Join(err, errors.New("generation: callback did not run"))
	}

	return errors.Join(err, callbackErr)
}

func panicError(value any) error {
	if cause, ok := value.(error); ok {
		return fmt.Errorf("generation: panic: %w", cause)
	}

	return fmt.Errorf("generation: panic: %v", value)
}
