package leadership

import (
	"testing"
	"time"
)

func TestBroadcaster(t *testing.T) {
	t.Parallel()

	broadcaster := NewBroadcaster[struct{}]()
	t.Cleanup(broadcaster.Close)

	const nbSubscriptions = 5

	subscriptions := make([]<-chan struct{}, nbSubscriptions)
	releases := make([]func(), nbSubscriptions)

	for i := 0; i < nbSubscriptions; i++ {
		subscriptions[i], releases[i] = broadcaster.Subscribe()
	}

	go broadcaster.Broadcast(struct{}{})

	for _, subscription := range subscriptions {
		select {
		case <-subscription:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for broadcast")
		}
	}

	releases[2]()
	subscriptions = append(subscriptions[:2], subscriptions[3:]...)
	releases = append(releases[:2], releases[3:]...)

	go broadcaster.Broadcast(struct{}{})

	for _, subscription := range subscriptions {
		select {
		case <-subscription:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for broadcast")
		}
	}

	releases[0]()
	subscriptions = subscriptions[1:]
	releases = releases[1:]

	go broadcaster.Broadcast(struct{}{})

	for _, subscription := range subscriptions {
		select {
		case <-subscription:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for broadcast")
		}
	}

	releases[2]()
	subscriptions = subscriptions[:2]

	go broadcaster.Broadcast(struct{}{})

	for _, subscription := range subscriptions {
		select {
		case <-subscription:
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for broadcast")
		}
	}
}
