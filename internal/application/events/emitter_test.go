package events

import (
	"testing"

	"github.com/formancehq/go-libs/v4/logging"
)

func TestEmitter_StopIdempotent(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	emitter := NewEmitter(nil, &NoopSink{}, "test-sink", nil, logger, DefaultEmitterConfig())

	// Stop on a never-started emitter should be a no-op
	emitter.Stop()
	emitter.Stop()
}

func TestEmitter_NonBlockingNotification(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	emitter := NewEmitter(nil, &NoopSink{}, "test-sink", nil, logger, DefaultEmitterConfig())

	// Non-blocking notify should not panic even when emitter is not running
	emitter.Notify()

	// Multiple notifications should not block (coalesced via Signal)
	emitter.Notify()
	emitter.Notify()
}
