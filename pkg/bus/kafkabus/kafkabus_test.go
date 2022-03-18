package kafkabus

import (
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/numary/ledger/pkg/bus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"testing"
)

func TestModuleKafka(t *testing.T) {

	broker := sarama.NewMockBroker(t, 12)
	defer broker.Close()

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(broker.Addr(), broker.BrokerID())
	broker.Returns(metadataResponse)

	app := fxtest.New(t,
		bus.Module(),
		Module("ledger", broker.Addr()),
		fx.Replace(sarama.MinVersion),
		fx.Invoke(func(p message.Publisher) {
			assert.IsType(t, &kafka.Publisher{}, p)
		}),
	)
	app.
		RequireStart().
		RequireStop()
}
