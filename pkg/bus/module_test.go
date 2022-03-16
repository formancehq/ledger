package bus

import (
	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/numary/ledger/pkg/bus/kafkabus"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"go.uber.org/fx/fxtest"
	"testing"
)

func TestModuleDefault(t *testing.T) {
	app := fxtest.New(t, Module())
	app.
		RequireStart().
		RequireStop()
}

func TestModuleKafka(t *testing.T) {

	broker := sarama.NewMockBroker(t, 12)
	defer broker.Close()

	metadataResponse := new(sarama.MetadataResponse)
	metadataResponse.AddBroker(broker.Addr(), broker.BrokerID())
	broker.Returns(metadataResponse)

	app := fxtest.New(t,
		Module(),
		kafkabus.Module("ledger", broker.Addr()),
		fx.Invoke(func(p message.Publisher, s message.Subscriber) {
			assert.IsType(t, &kafka.Publisher{}, p)
			assert.IsType(t, &kafka.Subscriber{}, s)
		}),
	)
	app.
		RequireStart().
		RequireStop()
}
