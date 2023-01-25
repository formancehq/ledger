package stripe

import (
	"context"
	"testing"
	"time"

	"github.com/stripe/stripe-go/v72"

	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
)

func TestStopTailing(t *testing.T) {
	t.Parallel()

	NoOpIngester := IngesterFn(func(ctx context.Context, batch []*stripe.BalanceTransaction,
		commitState TimelineState, tail bool,
	) error {
		return nil
	})

	mock := NewClientMock(t, true)
	timeline := NewTimeline(mock, TimelineConfig{
		PageSize: 2,
	}, TimelineState{
		OldestID:     "tx1",
		MoreRecentID: "tx2",
	})

	logger := logging.GetLogger(context.Background())
	trigger := NewTimelineTrigger(logger, NoOpIngester, timeline)
	r := NewRunner(logger, trigger, time.Second)

	go func() {
		_ = r.Run(context.Background())
	}()

	defer func() {
		_ = r.Stop(context.Background())
	}()

	require.False(t, timeline.state.NoMoreHistory)

	mock.Expect().RespondsWith(false) // Fetch head
	mock.Expect().RespondsWith(false) // Fetch tail

	require.Eventually(t, func() bool {
		return timeline.state.NoMoreHistory
	}, time.Second, 10*time.Millisecond)
}
