package publish

import (
	"context"
	"encoding/json"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/logging"
	"github.com/google/uuid"
	"go.uber.org/fx"
)

type Publisher interface {
	Publish(ctx context.Context, topic string, ev any) error
}

// TODO: Inject OpenTracing context
func newMessage(ctx context.Context, m any) *message.Message {
	data, err := json.Marshal(m)
	if err != nil {
		panic(err)
	}
	msg := message.NewMessage(uuid.NewString(), data)
	msg.SetContext(ctx)
	return msg
}

type TopicMapperPublisher struct {
	publisher message.Publisher
	topics    map[string]string
}

func (l *TopicMapperPublisher) publish(ctx context.Context, topic string, ev any) error {
	err := l.publisher.Publish(topic, newMessage(ctx, ev))
	if err != nil {
		logging.GetLogger(ctx).Errorf("Publishing message: %s", err)
		return err
	}
	return nil
}

func (l *TopicMapperPublisher) Publish(ctx context.Context, topic string, ev any) error {
	mappedTopic, ok := l.topics[topic]
	if ok {
		return l.publish(ctx, mappedTopic, ev)
	}
	mappedTopic, ok = l.topics["*"]
	if ok {
		return l.publish(ctx, mappedTopic, ev)
	}

	return l.publish(ctx, topic, ev)
}

func NewTopicMapperPublisher(publisher message.Publisher, topics map[string]string) *TopicMapperPublisher {
	return &TopicMapperPublisher{
		publisher: publisher,
		topics:    topics,
	}
}

var _ Publisher = &TopicMapperPublisher{}

func TopicMapperPublisherModule(topics map[string]string) fx.Option {
	return fx.Provide(func(p message.Publisher) *TopicMapperPublisher {
		return NewTopicMapperPublisher(p, topics)
	})
}
