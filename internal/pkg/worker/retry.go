package worker

import (
	"errors"
	"time"

	"github.com/formancehq/go-libs/v4/logging"
)

// ErrNotLeader is returned by worker functions when the current node is not the
// Raft leader. The retry loop uses this to log a leadership-specific message
// and wait before re-checking.
var ErrNotLeader = errors.New("not leader")

// RetryWithBackoff calls fn repeatedly with exponential backoff (100ms to 10s)
// until it succeeds (returns nil) or stop is closed. ErrNotLeader triggers a
// leadership-specific log message; other errors log a generic retry message.
func RetryWithBackoff(stop <-chan struct{}, logger logging.Logger, fn func() error) {
	backoff := 100 * time.Millisecond

	const maxBackoff = 10 * time.Second

	for {
		err := fn()
		if err == nil {
			return
		}

		if errors.Is(err, ErrNotLeader) {
			logger.Infof("Not leader, waiting %v before re-checking", backoff)
		} else {
			logger.Errorf("Failed (will retry in %v): %v", backoff, err)
		}

		select {
		case <-stop:
			return
		case <-time.After(backoff):
		}

		backoff = min(backoff*2, maxBackoff)
	}
}
