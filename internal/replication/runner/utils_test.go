package runner

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/stretchr/testify/require"
)

func AlwaysEmptySubscription(ctx context.Context) (chan *message.Message, error) {
	ret := make(chan *message.Message)
	go func() {
		<-ctx.Done()
		close(ret)
	}()
	return ret, nil
}

//func newFakeSubscriber(ctx context.Context, to chan *message.Message) (<-chan *message.Message, error) {
//	go func() {
//		<-ctx.Done()
//		close(to)
//	}()
//	return to, nil
//}
//
//func newFakeSubscriberFactory(to chan *message.Message) func(context.Context) (<-chan *message.Message, error) {
//	return func(ctx context.Context) (<-chan *message.Message, error) {
//		return newFakeSubscriber(ctx, to)
//	}
//}

func ShouldReceive[T any](t *testing.T, expected T, ch <-chan T) {
	t.Helper()

	require.Eventually(t, func() bool {
		select {
		case item := <-ch:
			require.Equal(t, expected, item)
			return true
		case <-time.After(time.Second):
			return false
		}
	}, time.Second, 20*time.Millisecond)
}
