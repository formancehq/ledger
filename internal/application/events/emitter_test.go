package events

import (
	"testing"

	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestEmitter_StopIdempotent(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	sink := NewMockSink(ctrl)

	logger := logging.Testing()
	emitter := NewEmitter(nil, sink, "test-sink", nil, logger, DefaultEmitterConfig())

	// Stop on a never-started emitter should be a no-op
	emitter.Stop()
	emitter.Stop()
}

func TestEmitter_NonBlockingNotification(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	sink := NewMockSink(ctrl)

	logger := logging.Testing()
	emitter := NewEmitter(nil, sink, "test-sink", nil, logger, DefaultEmitterConfig())

	// Non-blocking notify should not panic even when emitter is not running
	emitter.Notify()

	// Multiple notifications should not block (coalesced via Signal)
	emitter.Notify()
	emitter.Notify()
}
