package worker

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestChannelTrySend(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())
	ch := NewChannel[int](logger, "test", 1)

	// First send should succeed — buffer has capacity.
	assert.True(t, ch.TrySend(42, "first item"))

	// Second send should fail — buffer is full.
	assert.False(t, ch.TrySend(99, "second item"))
}

func TestChannelSend(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())
	ch := NewChannel[int](logger, "test", 0) // unbuffered

	stop := make(chan struct{})

	// Send should block until someone receives or stop is closed.
	// Close stop to unblock.
	go func() {
		close(stop)
	}()

	sent := ch.Send(1, stop)
	assert.False(t, sent, "Send should return false when stop is closed")

	// Now test the successful path: start a receiver, then send.
	stop2 := make(chan struct{})

	received := make(chan int, 1)

	go func() {
		v := <-ch.Receive()
		received <- v
	}()

	sent = ch.Send(7, stop2)
	require.True(t, sent)
	require.Equal(t, 7, <-received)
}

func TestChannelReceive(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())
	ch := NewChannel[string](logger, "test", 1)

	ch.TrySend("hello", "test")

	recv := ch.Receive()
	require.NotNil(t, recv)

	val := <-recv
	assert.Equal(t, "hello", val)
}
