package bus

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/numary/go-libs/sharedpublish"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/assert"
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
	p := sharedpublish.NewTopicMapperPublisher(pubSub, map[string]string{
		"*": "testing",
	})
	m := NewLedgerMonitor(p)
	go m.CommittedTransactions(context.Background(), uuid.New(), nil, nil)

	select {
	case m := <-messages:
		m.Ack()
	case <-time.After(time.Second):
		t.Fatal("should have a message")
	}

}
