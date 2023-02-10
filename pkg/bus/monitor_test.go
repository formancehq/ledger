package bus

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/formancehq/stack/libs/go-libs/publish"
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
	p := publish.NewTopicMapperPublisher(pubSub, map[string]string{
		"*": "testing",
	})
	m := newLedgerMonitor(p)
	go m.CommittedTransactions(context.Background(), uuid.New())

	select {
	case m := <-messages:
		m.Ack()
	case <-time.After(time.Second):
		t.Fatal("should have a message")
	}

}
