package topicmapper

import (
	"github.com/ThreeDotsLabs/watermill/message"
)

type TopicMapperPublisherDecorator struct {
	message.Publisher
	topics map[string]string
}

func (p TopicMapperPublisherDecorator) Publish(topic string, messages ...*message.Message) error {
	mappedTopic, ok := p.topics[topic]
	if ok {
		return p.Publisher.Publish(mappedTopic, messages...)
	}
	mappedTopic, ok = p.topics["*"]
	if ok {
		return p.Publisher.Publish(mappedTopic, messages...)
	}

	return p.Publisher.Publish(topic, messages...)
}

var _ message.Publisher = &TopicMapperPublisherDecorator{}

func NewPublisherDecorator(publisher message.Publisher, topics map[string]string) *TopicMapperPublisherDecorator {
	return &TopicMapperPublisherDecorator{
		Publisher: publisher,
		topics:    topics,
	}
}
