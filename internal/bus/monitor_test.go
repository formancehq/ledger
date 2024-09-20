package bus

import (
	"context"
	"testing"
	"time"

	ledger "github.com/formancehq/ledger/internal"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	topicmapper "github.com/formancehq/go-libs/publish/topic_mapper"
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
	p := topicmapper.NewPublisherDecorator(pubSub, map[string]string{
		"*": "testing",
	})
	m := NewLedgerMonitor(p, uuid.New())
	go m.CommittedTransactions(context.Background(), ledger.Transaction{}, nil)

	select {
	case m := <-messages:
		m.Ack()
	case <-time.After(time.Second):
		t.Fatal("should have a message")
	}

}
