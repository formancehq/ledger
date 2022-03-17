package bus

import (
	"context"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestMonitor(t *testing.T) {

	pubSub := gochannel.NewGoChannel(
		gochannel.Config{
			BlockPublishUntilSubscriberAck: true,
		},
		watermill.NewStdLogger(false, false),
	)
	messages, err := pubSub.Subscribe(context.Background(), "testing")
	if !assert.NoError(t, err) {
		return
	}
	m := NewLedgerMonitor(pubSub, WithLedgerMonitorGlobalTopic("testing"))
	go m.CommittedTransactions(context.Background(), uuid.New(), nil, nil)

	select {
	case m := <-messages:
		m.Ack()
	case <-time.After(time.Second):
		t.Fatal("should have a message")
	}

}
