package circuitbreaker

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCircuitBreaker(t *testing.T) {
	t.Parallel()

	t.Run("nominal", func(t *testing.T) {
		t.Parallel()

		messages := make(chan *testMessages, 100)
		defer close(messages)

		publisher := newCircuitBreaker(
			logging.Testing(),
			newMockPublisher(messages),
			newMockStore(),
			5*time.Second,
		)
		defer publisher.Close()

		go publisher.loop()

		expectedP1, _ := json.Marshal(&payload{Result: 1})
		err := publisher.Publish("test", message.NewMessage("1", expectedP1))
		require.NoError(t, err)
		require.Equal(t, StateClose, publisher.state)

		expectedP2, _ := json.Marshal(&payload{Result: 2})
		err = publisher.Publish("test", message.NewMessage("2", expectedP2))
		require.NoError(t, err)
		require.Equal(t, StateClose, publisher.state)

		expectedP3, _ := json.Marshal(&payload{Result: 3})
		err = publisher.Publish("test", message.NewMessage("3", expectedP3))
		require.NoError(t, err)
		require.Equal(t, StateClose, publisher.state)

		require.Len(t, messages, 3)
		msg1 := <-messages
		require.Equal(t, "test", msg1.topic)
		require.Equal(t, "1", msg1.msg.UUID)
		p1 := &payload{}
		_ = json.Unmarshal(msg1.msg.Payload, p1)
		require.Equal(t, 1, p1.Result)

		msg2 := <-messages
		require.Equal(t, "test", msg2.topic)
		require.Equal(t, "2", msg2.msg.UUID)
		p2 := &payload{}
		_ = json.Unmarshal(msg2.msg.Payload, p2)
		require.Equal(t, 2, p2.Result)

		msg3 := <-messages
		require.Equal(t, "test", msg3.topic)
		require.Equal(t, "3", msg3.msg.UUID)
		p3 := &payload{}
		_ = json.Unmarshal(msg3.msg.Payload, p3)
		require.Equal(t, 3, p3.Result)
	})

	t.Run("error publisher", func(t *testing.T) {
		t.Parallel()

		messages := make(chan *testMessages, 100)
		defer close(messages)

		errTest := errors.New("test")
		underlyingPublisher := newMockPublisher(messages)
		store := newMockStore()
		publisher := newCircuitBreaker(
			logging.Testing(),
			underlyingPublisher,
			store,
			5*time.Second,
		)
		defer publisher.Close()

		go publisher.loop()

		expectedP1, _ := json.Marshal(&payload{Result: 1})
		m1 := message.NewMessage("1", expectedP1)
		m1.Metadata.Set("foo", "bar")
		err := publisher.Publish("test", m1)
		require.NoError(t, err)
		require.Equal(t, StateClose, publisher.state)

		underlyingPublisher.WithPublishError(errTest)

		expectedP2, _ := json.Marshal(&payload{Result: 2})
		m2 := message.NewMessage("2", expectedP2)
		m2.Metadata.Set("foo", "bar")
		err = publisher.Publish("test", m2)
		require.NoError(t, err)
		require.Equal(t, StateOpen, publisher.state)

		expectedP3, _ := json.Marshal(&payload{Result: 3})
		m3 := message.NewMessage("3", expectedP3)
		m3.Metadata.Set("foo2", "bar2")
		err = publisher.Publish("test", m3)
		require.NoError(t, err)
		require.Equal(t, StateOpen, publisher.state)

		require.Len(t, messages, 1)
		msg1 := <-messages
		require.Equal(t, "test", msg1.topic)
		require.Equal(t, "1", msg1.msg.UUID)
		require.Equal(t, message.Metadata(map[string]string{"foo": "bar"}), msg1.msg.Metadata)
		p1 := &payload{}
		_ = json.Unmarshal(msg1.msg.Payload, p1)
		require.Equal(t, 1, p1.Result)

		storedMessages, err := store.List(context.Background())
		require.NoError(t, err)
		require.Len(t, storedMessages, 2)

		require.Equal(t, "test", storedMessages[0].Topic)
		require.Equal(t, map[string]string{"foo": "bar"}, storedMessages[0].Metadata)
		p2 := &payload{}
		_ = json.Unmarshal(storedMessages[0].Data, p2)
		require.Equal(t, 2, p2.Result)

		require.Equal(t, "test", storedMessages[1].Topic)
		require.Equal(t, map[string]string{"foo2": "bar2"}, storedMessages[1].Metadata)
		p3 := &payload{}
		_ = json.Unmarshal(storedMessages[1].Data, p3)
		require.Equal(t, 3, p3.Result)
	})

	t.Run("error publisher and store", func(t *testing.T) {
		t.Parallel()

		messages := make(chan *testMessages, 100)
		defer close(messages)

		errTest := errors.New("test")
		underlyingPublisher := newMockPublisher(messages)
		store := newMockStore().WithInsertError(errTest)
		publisher := newCircuitBreaker(
			logging.Testing(),
			underlyingPublisher,
			store,
			5*time.Second,
		)
		defer publisher.Close()

		go publisher.loop()

		expectedP1, _ := json.Marshal(&payload{Result: 1})
		m1 := message.NewMessage("1", expectedP1)
		m1.Metadata.Set("foo", "bar")
		err := publisher.Publish("test", m1)
		require.NoError(t, err)
		require.Equal(t, StateClose, publisher.state)

		underlyingPublisher.WithPublishError(errTest)

		expectedP2, _ := json.Marshal(&payload{Result: 2})
		m2 := message.NewMessage("2", expectedP2)
		m2.Metadata.Set("foo", "bar")
		err = publisher.Publish("test", m2)
		require.ErrorIs(t, err, errTest)
		require.Equal(t, StateOpen, publisher.state)

		expectedP3, _ := json.Marshal(&payload{Result: 3})
		m3 := message.NewMessage("3", expectedP3)
		m3.Metadata.Set("foo2", "bar2")
		err = publisher.Publish("test", m3)
		require.ErrorIs(t, err, errTest)
		require.Equal(t, StateOpen, publisher.state)

		require.Len(t, messages, 1)
		msg1 := <-messages
		require.Equal(t, "test", msg1.topic)
		require.Equal(t, "1", msg1.msg.UUID)
		require.Equal(t, message.Metadata(map[string]string{"foo": "bar"}), msg1.msg.Metadata)
		p1 := &payload{}
		_ = json.Unmarshal(msg1.msg.Payload, p1)
		require.Equal(t, 1, p1.Result)

		storedMessages, err := store.List(context.Background())
		require.NoError(t, err)
		require.Len(t, storedMessages, 0)
	})

	t.Run("error publisher but recover after x seconds", func(t *testing.T) {
		t.Parallel()

		messages := make(chan *testMessages, 100)
		defer close(messages)

		errTest := errors.New("test")
		underlyingPublisher := newMockPublisher(messages)
		store := newMockStore()
		publisher := newCircuitBreaker(
			logging.Testing(),
			underlyingPublisher,
			store,
			5*time.Second,
		)
		defer publisher.Close()

		go publisher.loop()

		expectedP1, _ := json.Marshal(&payload{Result: 1})
		m1 := message.NewMessage("1", expectedP1)
		m1.Metadata.Set("foo", "bar")
		err := publisher.Publish("test", m1)
		require.NoError(t, err)
		require.Equal(t, StateClose, publisher.state)

		underlyingPublisher.WithPublishError(errTest)

		expectedP2, _ := json.Marshal(&payload{Result: 2})
		m2 := message.NewMessage("2", expectedP2)
		m2.Metadata.Set("foo", "bar")
		err = publisher.Publish("test", m2)
		require.NoError(t, err)
		require.Equal(t, StateOpen, publisher.state)

		expectedP3, _ := json.Marshal(&payload{Result: 3})
		m3 := message.NewMessage("3", expectedP3)
		m3.Metadata.Set("foo2", "bar2")
		err = publisher.Publish("test", m3)
		require.NoError(t, err)
		require.Equal(t, StateOpen, publisher.state)

		require.Len(t, messages, 1)
		msg1 := <-messages
		require.Equal(t, "test", msg1.topic)
		require.Equal(t, "1", msg1.msg.UUID)
		require.Equal(t, message.Metadata(map[string]string{"foo": "bar"}), msg1.msg.Metadata)
		p1 := &payload{}
		_ = json.Unmarshal(msg1.msg.Payload, p1)
		require.Equal(t, 1, p1.Result)

		storedMessages, err := store.List(context.Background())
		require.NoError(t, err)
		require.Len(t, storedMessages, 2)

		require.Equal(t, "test", storedMessages[0].Topic)
		require.Equal(t, map[string]string{"foo": "bar"}, storedMessages[0].Metadata)
		p2 := &payload{}
		_ = json.Unmarshal(storedMessages[0].Data, p2)
		require.Equal(t, 2, p2.Result)

		require.Equal(t, "test", storedMessages[1].Topic)
		require.Equal(t, map[string]string{"foo2": "bar2"}, storedMessages[1].Metadata)
		p3 := &payload{}
		_ = json.Unmarshal(storedMessages[1].Data, p3)
		require.Equal(t, 3, p3.Result)

		underlyingPublisher.WithPublishError(nil)

		require.EventuallyWithT(t, func(c *assert.CollectT) {
			if !assert.Equal(c, StateClose, publisher.state) {
				return
			}

			// Now we must fail if state is closed and there is nothing or wrong
			// data in the messages channel

			require.Len(t, messages, 2)

			msg2 := <-messages
			require.Equal(t, "test", msg2.topic)
			require.Equal(t, message.Metadata(map[string]string{"foo": "bar"}), msg2.msg.Metadata)
			p2 := &payload{}
			_ = json.Unmarshal(msg2.msg.Payload, p2)
			require.Equal(t, 2, p2.Result)

			msg3 := <-messages
			require.Equal(t, "test", msg3.topic)
			require.Equal(t, message.Metadata(map[string]string{"foo2": "bar2"}), msg3.msg.Metadata)
			p3 := &payload{}
			_ = json.Unmarshal(msg3.msg.Payload, p3)
			require.Equal(t, 3, p3.Result)

			storedMessages, err := store.List(context.Background())
			require.NoError(t, err)
			require.Len(t, storedMessages, 0)
		}, 10*time.Second, 1*time.Second)
	})
}
