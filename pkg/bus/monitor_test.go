package bus

import (
	"context"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/formancehq/stack/libs/go-libs/publish"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

func TestMonitor(t *testing.T) {

	pubSub := gochannel.NewGoChannel(
		gochannel.Config{
			BlockPublishUntilSubscriberAck: true,
		},
		watermill.NewStdLogger(testing.Verbose(), testing.Verbose()),
	)
	messages, err := pubSub.Subscribe(context.Background(), "testing")
	require.NoError(t, err)
	p := publish.NewTopicMapperPublisherDecorator(pubSub, map[string]string{
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
