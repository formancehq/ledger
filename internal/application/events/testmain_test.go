package events_test

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
)

var topicCounter atomic.Int64

// uniqueTopic returns a unique topic name for each test invocation,
// preventing cross-invocation message contamination when tests run
// in parallel or with -count > 1.
func uniqueTopic(prefix string) string {
	return fmt.Sprintf("%s_%d", prefix, topicCounter.Add(1))
}

// testSetupFunc is registered by build-tagged init() functions to set up
// test infrastructure (e.g., Kafka/ClickHouse containers).
type testSetupFunc struct {
	setup    func(ctx context.Context) error
	teardown func(ctx context.Context)
}

var testSetups []testSetupFunc

func registerTestSetup(setup func(ctx context.Context) error, teardown func(ctx context.Context)) {
	testSetups = append(testSetups, testSetupFunc{setup: setup, teardown: teardown})
}

func TestMain(m *testing.M) {
	ctx := context.Background()

	for _, s := range testSetups {
		if err := s.setup(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "test setup failed: %v\n", err)
			os.Exit(1)
		}
	}

	code := m.Run()

	for _, s := range testSetups {
		s.teardown(ctx)
	}

	os.Exit(code)
}
