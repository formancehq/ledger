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

	t.Run("lazy row production is charged to execution and stays in the server total", func(t *testing.T) {
		t.Parallel()

		// This is the regression that matters. Nothing sends x-query-profile in
		// normal operation, so the slow-query log is the profile's only consumer;
		// an earlier revision gated the per-row split on the caller having asked
		// for the profile and charged the whole loop to delivery otherwise. Since
		// Finish() subtracts delivery, a forwarded read that spent seconds inside
		// cur.Next() reported a sub-millisecond ServerDuration — the log was blind
		// in the exact configuration that uses it.
		ctx, profile := query.WithProfile(context.Background())
		profile.EnterExecute()
		profile.LeaveExecute()

		stream := newFakeServerStream[stringItem](t)

		err := sendPagedToStream(
			ctx, &spinCursor{remaining: 3, per: spinPerItem}, stream, "item", 0, nil,
		)
		require.NoError(t, err)
		require.Len(t, stream.sent, 3)

		// 3 items + the EOF probe all spin, so at least 3 spins are attributed.
		require.GreaterOrEqual(t, profile.ExecuteDuration, 3*spinPerItem,
			"cur.Next() time must be attributed to the execution phase")

		profile.Finish()

		require.GreaterOrEqual(t, profile.ServerDuration, 3*spinPerItem,
			"row production must survive into ServerDuration; if it does not, the "+
				"slow-query threshold cannot see a slow forwarded read")
		require.GreaterOrEqual(t, profile.WallDuration(), profile.ServerDuration,
			"the wall total includes delivery on top of the server total")
	})

	t.Run("stream sends are charged to delivery, outside the server total", func(t *testing.T) {
		t.Parallel()

		ctx, profile := query.WithProfile(context.Background())
		profile.EnterExecute()
		profile.LeaveExecute()

		stream := newFakeServerStream[stringItem](t)
		stream.sendDelay = spinPerItem

		err := sendPagedToStream(
			ctx, &spinCursor{remaining: 3, per: 0}, stream, "item", 0, nil,
		)
		require.NoError(t, err)

		require.GreaterOrEqual(t, profile.DeliverDuration, 3*spinPerItem,
			"stream.Send() time must be attributed to the delivery phase")

		profile.Finish()

		require.Less(t, profile.ServerDuration, 3*spinPerItem,
			"consumer back-pressure must not inflate the consumer-independent total")
		require.GreaterOrEqual(t, profile.WallDuration(), 3*spinPerItem,
			"but the slow-query threshold must still see it")
		require.Positive(t, profile.FirstRowDuration, "the first Send must be timestamped")
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
