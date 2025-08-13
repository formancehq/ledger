package nats

import (
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/nats-io/nats.go"
	"github.com/pkg/errors"
)

// PublisherConfig is the configuration to create a publisher
type PublisherConfig struct {
	// URL is the NATS URL.
	URL string

	// NatsOptions are custom options for a connection.
	NatsOptions []nats.Option

	// Marshaler is marshaler used to marshal messages between watermill and wire formats
	Marshaler Marshaler

	// SubjectCalculator is a function used to transform a topic to an array of subjects on creation (defaults to topic as Primary and queueGroupPrefix as QueueGroup)
	SubjectCalculator SubjectCalculator

	// JetStream holds JetStream specific settings
	JetStream JetStreamConfig
}

// PublisherPublishConfig is the configuration subset needed for an individual publish call
type PublisherPublishConfig struct {
	// Marshaler is marshaler used to marshal messages between watermill and wire formats
	Marshaler Marshaler

	// SubjectCalculator is a function used to transform a topic to an array of subjects on creation (defaults to topic as Primary and queueGroupPrefix as QueueGroup)
	SubjectCalculator SubjectCalculator

	// JetStream holds JetStream specific settings
	JetStream JetStreamConfig
}

func (c *PublisherConfig) setDefaults() {
	if c.Marshaler == nil {
		c.Marshaler = &NATSMarshaler{}
	}
	if c.SubjectCalculator == nil {
		c.SubjectCalculator = DefaultSubjectCalculator
	}
}

// Validate ensures configuration is valid before use
func (c PublisherConfig) Validate() error {
	if c.Marshaler == nil {
		return errors.New("PublisherConfig.Marshaler is missing")
	}

	if c.SubjectCalculator == nil {
		return errors.New("PublisherConfig.SubjectCalculator is missing")
	}
	return nil
}

// GetPublisherPublishConfig gets the configuration subset needed for individual publish calls once a connection has been established
func (c PublisherConfig) GetPublisherPublishConfig() PublisherPublishConfig {
	return PublisherPublishConfig{
		Marshaler:         c.Marshaler,
		SubjectCalculator: c.SubjectCalculator,
		JetStream:         c.JetStream,
	}
}

// Publisher provides the nats implementation for watermill publish operations
type Publisher struct {
	conn             Connection
	config           PublisherPublishConfig
	logger           watermill.LoggerAdapter
	topicInterpreter *topicInterpreter
}

// NewPublisher creates a new Publisher.
func NewPublisher(config PublisherConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	config.setDefaults()

	if err := config.Validate(); err != nil {
		return nil, err
	}

	conn, err := nats.Connect(config.URL, config.NatsOptions...)
	if err != nil {
		return nil, errors.Wrap(err, "cannot connect to nats")
	}

	return NewPublisherWithNatsConn(conn, config.GetPublisherPublishConfig(), logger)
}

// NewPublisherWithNatsConn creates a new Publisher with the provided nats connection.
func NewPublisherWithNatsConn(conn *nats.Conn, config PublisherPublishConfig, logger watermill.LoggerAdapter) (*Publisher, error) {
	if logger == nil {
		logger = watermill.NopLogger{}
	}

	var connection Connection = conn
	var interpreter *topicInterpreter

	if !config.JetStream.Disabled {
		js, err := conn.JetStream(config.JetStream.ConnectOptions...)

		connection = &jsConnection{conn, js, config.JetStream}

		if err != nil {
			return nil, err
		}

		interpreter = newTopicInterpreter(js, config.SubjectCalculator, "")
	}

	return &Publisher{
		conn:             connection,
		config:           config,
		logger:           logger,
		topicInterpreter: interpreter,
	}, nil
}

// Publish publishes message to NATS.
//
// Publish will not return until an ack has been received from JetStream.
// When one of messages delivery fails - function is interrupted.
func (p *Publisher) Publish(topic string, messages ...*message.Message) error {
	// TODO: should we auto provision on publish?  Need durable on publish options...
	// should also cache this result to minimize chatter to broker
	if p.config.JetStream.ShouldAutoProvision() {
		err := p.topicInterpreter.ensureStream(topic)
		if err != nil {
			return err
		}
	}

	for _, msg := range messages {
		messageFields := watermill.LogFields{
			"message_uuid": msg.UUID,
			"topic_name":   topic,
		}

		p.logger.Trace("Publishing message", messageFields)

		natsMsg, err := p.config.Marshaler.Marshal(topic, msg)
		if err != nil {
			return err
		}

		if err := p.conn.PublishMsg(natsMsg); err != nil {
			return errors.Wrap(err, "sending message failed")
		}
	}

	return nil
}

// Close closes the publisher and the underlying connection
func (p *Publisher) Close() error {
	p.logger.Trace("Closing publisher", nil)
	defer p.logger.Trace("Publisher closed", nil)

	p.conn.Close()

	return nil
}
