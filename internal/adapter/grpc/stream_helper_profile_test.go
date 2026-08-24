package grpc

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/query"
)

// spinCursor emits a fixed number of items, burning a measurable amount of
// wall-clock inside each Next(). It stands in for the routed-read case where
// rows are produced lazily from the leader's stream *inside* the send loop:
// that cost is query execution, not delivery, and the profile has to say so.
//
// It spins rather than sleeps: the test needs elapsed monotonic time it can
// assert on, and a busy wait keeps the measurement independent of scheduler
// wake-up granularity.
type spinCursor struct {
	remaining int
	per       time.Duration
}

func (c *spinCursor) Next() (*stringItem, error) {
	if c.remaining == 0 {
		return nil, errIOEOF
	}

	c.remaining--

	for deadline := time.Now().Add(c.per); time.Now().Before(deadline); {
		runtime.Gosched()
	}

	return &stringItem{name: "item"}, nil
}

func (c *spinCursor) Close() error { return nil }

const spinPerItem = 2 * time.Millisecond

func TestSendPagedToStream_ProfilePhaseAttribution(t *testing.T) {
	t.Parallel()

	t.Run("detailed → row production is charged to execution, not delivery", func(t *testing.T) {
		t.Parallel()

		ctx, profile := query.WithProfile(context.Background(), true)
		profile.EnterExecute()
		profile.LeaveExecute()

		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(
			ctx, &spinCursor{remaining: 3, per: spinPerItem}, stream, "item", 0, nil,
		)
		require.NoError(t, err)
		require.Len(t, stream.sent, 3)

		// 3 items + the EOF probe all spin, so at least 3 spins land in the
		// execution phase.
		require.GreaterOrEqual(t, profile.ExecuteDuration, 3*spinPerItem,
			"cur.Next() time must be attributed to the execution phase")

		profile.Finish()
		require.Positive(t, profile.FirstRowDuration, "the first Send must be timestamped")
		require.LessOrEqual(t, profile.FirstRowDuration, profile.ServerDuration+profile.DeliverDuration+
			profile.BarrierDuration, "time-to-first-row cannot exceed the whole request")
	})

	t.Run("not detailed → the whole loop is charged to delivery", func(t *testing.T) {
		t.Parallel()

		ctx, profile := query.WithProfile(context.Background(), false)
		profile.EnterExecute()
		profile.LeaveExecute()

		executeAfterQuery := profile.ExecuteDuration

		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(
			ctx, &spinCursor{remaining: 3, per: spinPerItem}, stream, "item", 0, nil,
		)
		require.NoError(t, err)

		// A request that did not ask for the profile pays no per-row clock
		// reads, so the loop cannot be split: it is charged wholly to delivery.
		// That is the conservative direction — delivery is excluded from
		// ServerDuration, so the server total can only be under-reported, never
		// inflated by a slow consumer.
		require.Equal(t, executeAfterQuery, profile.ExecuteDuration,
			"no per-row production attribution without an explicit profile request")
		require.GreaterOrEqual(t, profile.DeliverDuration, 3*spinPerItem,
			"the whole send loop must still be accounted for somewhere")

		profile.Finish()
		require.Positive(t, profile.FirstRowDuration,
			"time-to-first-row is O(1) and always collected")
	})

	t.Run("no profile in context → helper still streams", func(t *testing.T) {
		t.Parallel()

		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(
			context.Background(), &spinCursor{remaining: 2, per: 0}, stream, "item", 0, nil,
		)
		require.NoError(t, err)
		require.Len(t, stream.sent, 2)
	})
}
