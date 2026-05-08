package events

import (
	"context"
	"testing"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
)

type noopSink struct{}

func (noopSink) Publish(context.Context, []*eventspb.Event) error { return nil }
func (noopSink) Close() error                                     { return nil }

func TestEmitter_StopIdempotent(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	emitter := NewEmitter(nil, &noopSink{}, "test-sink", nil, logger, DefaultEmitterConfig())

	// Stop on a never-started emitter should be a no-op
	emitter.Stop()
	emitter.Stop()
}

func TestEmitter_NonBlockingNotification(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	emitter := NewEmitter(nil, &noopSink{}, "test-sink", nil, logger, DefaultEmitterConfig())

	// Non-blocking notify should not panic even when emitter is not running
	emitter.Notify()

	// Multiple notifications should not block (coalesced via Signal)
	emitter.Notify()
	emitter.Notify()
}
