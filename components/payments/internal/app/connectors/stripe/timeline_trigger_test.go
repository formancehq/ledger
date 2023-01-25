package stripe

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
	"github.com/stripe/stripe-go/v72"
)

func TestTimelineTrigger(t *testing.T) {
	t.Parallel()

	const txCount = 12

	mock := NewClientMock(t, true)
	ref := time.Now().Add(-time.Minute * time.Duration(txCount) / 2)
	timeline := NewTimeline(mock, TimelineConfig{
		PageSize: 2,
	}, TimelineState{}, WithStartingAt(ref))

	ingestedTx := make([]*stripe.BalanceTransaction, 0)
	trigger := NewTimelineTrigger(
		logging.GetLogger(context.Background()),
		IngesterFn(func(ctx context.Context, batch []*stripe.BalanceTransaction, commitState TimelineState, tail bool) error {
			ingestedTx = append(ingestedTx, batch...)

			return nil
		}),
		timeline,
	)

	allTxs := make([]*stripe.BalanceTransaction, txCount)
	for i := 0; i < txCount/2; i++ {
		allTxs[txCount/2+i] = &stripe.BalanceTransaction{
			ID:      fmt.Sprintf("%d", txCount/2+i),
			Created: ref.Add(-time.Duration(i) * time.Minute).Unix(),
		}
		allTxs[txCount/2-i-1] = &stripe.BalanceTransaction{
			ID:      fmt.Sprintf("%d", txCount/2-i-1),
			Created: ref.Add(time.Duration(i) * time.Minute).Unix(),
		}
	}

	for i := 0; i < txCount/2; i += 2 {
		mock.Expect().Limit(2).RespondsWith(i < txCount/2-2, allTxs[txCount/2+i], allTxs[txCount/2+i+1])
	}

	for i := 0; i < txCount/2; i += 2 {
		mock.Expect().Limit(2).RespondsWith(i < txCount/2-2, allTxs[txCount/2-i-2], allTxs[txCount/2-i-1])
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Second))
	defer cancel()

	require.NoError(t, trigger.Fetch(ctx))
	require.Len(t, ingestedTx, txCount)
}

func TestCancelTimelineTrigger(t *testing.T) {
	t.Parallel()

	const txCount = 12

	mock := NewClientMock(t, false)
	ref := time.Now().Add(-time.Minute * time.Duration(txCount) / 2)
	timeline := NewTimeline(mock, TimelineConfig{
		PageSize: 1,
	}, TimelineState{}, WithStartingAt(ref))

	waiting := make(chan struct{})
	trigger := NewTimelineTrigger(
		logging.GetLogger(context.Background()),
		IngesterFn(func(ctx context.Context, batch []*stripe.BalanceTransaction, commitState TimelineState, tail bool) error {
			close(waiting) // Instruct the test the trigger is in fetching state
			<-ctx.Done()

			return nil
		}),
		timeline,
	)

	allTxs := make([]*stripe.BalanceTransaction, txCount)
	for i := 0; i < txCount; i++ {
		allTxs[i] = &stripe.BalanceTransaction{
			ID: fmt.Sprintf("%d", i),
		}
		mock.Expect().Limit(1).RespondsWith(i < txCount-1, allTxs[i])
	}

	go func() {
		// TODO: Handle error
		_ = trigger.Fetch(context.Background())
	}()
	select {
	case <-time.After(time.Second):
		t.Fatalf("timeout")
	case <-waiting:
		trigger.Cancel(context.Background())
		require.NotEmpty(t, mock.expectations)
	}
}
