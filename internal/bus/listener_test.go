package bus

import (
	"os"
	"testing"
	"time"

	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/pubsub/gochannel"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"

	topicmapper "github.com/formancehq/go-libs/v4/publish/topic_mapper"

	ledger "github.com/formancehq/ledger/internal"
)

func TestMonitor(t *testing.T) {

	pubSub := gochannel.NewGoChannel(
		gochannel.Config{
			BlockPublishUntilSubscriberAck: true,
		},
		watermill.NewStdLogger(os.Getenv("DEBUG") == "true", os.Getenv("DEBUG") == "true"),
	)
	messages, err := pubSub.Subscribe(t.Context(), "testing")
	require.NoError(t, err)
	p := topicmapper.NewPublisherDecorator(pubSub, map[string]string{
		"*": "testing",
	})
	m := NewLedgerListener(p)
	go m.CommittedTransactions(t.Context(), uuid.New(), ledger.Transaction{}, nil)

	select {
	case m := <-messages:
		m.Ack()
	case <-time.After(time.Second):
		t.Fatal("should have a message")
	}
}
