package nats

import (
	"github.com/nats-io/nats.go"
)

type topicInterpreter struct {
	js                nats.JetStreamManager
	subjectCalculator SubjectCalculator
	queueGroupPrefix  string
}

func newTopicInterpreter(js nats.JetStreamManager, formatter SubjectCalculator, queueGroupPrefix string) *topicInterpreter {
	if formatter == nil {
		// this should always be setup to the default
		panic("no subject calculator")
	}

	return &topicInterpreter{
		js:                js,
		subjectCalculator: formatter,
		queueGroupPrefix:  queueGroupPrefix,
	}
}

func (b *topicInterpreter) ensureStream(topic string) error {
	_, err := b.js.StreamInfo(topic)

	if err != nil {
		// TODO: provision durable as well
		// or simply provide override capability
		_, err = b.js.AddStream(&nats.StreamConfig{
			Name:        topic,
			Description: "",
			Subjects:    b.subjectCalculator(b.queueGroupPrefix, topic).All(),
		})

		if err != nil {
			return err
		}
	}

	return err
}
