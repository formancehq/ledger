//go:build kafka

package events

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/IBM/sarama"
	"github.com/xdg-go/scram"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func init() {
	registerSinkFactory("kafka", func(sc *commonpb.SinkConfig, format Format) (Sink, error) {
		s := sc.GetType().(*commonpb.SinkConfig_Kafka)

		return NewKafkaSink(KafkaSinkConfig{
			Brokers:       s.Kafka.GetBrokers(),
			Topic:         s.Kafka.GetTopic(),
			TLS:           s.Kafka.GetTls(),
			SASLMechanism: s.Kafka.GetSaslMechanism(),
			SASLUsername:  s.Kafka.GetSaslUsername(),
			SASLPassword:  s.Kafka.GetSaslPassword(),
			Format:        format,
		})
	})
}

// KafkaSinkConfig holds configuration for the Kafka sink.
type KafkaSinkConfig struct {
	Brokers       []string
	Topic         string
	TLS           bool
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
	Format        Format
}

// KafkaSink publishes events to Apache Kafka.
type KafkaSink struct {
	producer sarama.AsyncProducer
	topic    string
	format   Format

	closeMu sync.Mutex
	closed  bool
	closing chan struct{}
	active  sync.WaitGroup
	done    chan struct{}
}

// NewKafkaSink creates a new Kafka sink.
func NewKafkaSink(cfg KafkaSinkConfig) (*KafkaSink, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.Return.Successes = true
	saramaCfg.Producer.Return.Errors = true

	if cfg.TLS {
		saramaCfg.Net.TLS.Enable = true
		saramaCfg.Net.TLS.Config = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	if err := configureSASL(saramaCfg, cfg); err != nil {
		return nil, err
	}

	producer, err := sarama.NewAsyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("creating Kafka producer: %w", err)
	}

	sink := &KafkaSink{
		producer: producer,
		topic:    cfg.Topic,
		format:   cfg.Format,
		closing:  make(chan struct{}),
		done:     make(chan struct{}),
	}

	go sink.dispatchDeliveries()

	return sink, nil
}

func (s *KafkaSink) Publish(ctx context.Context, events []*eventspb.Event) error {
	if !s.beginPublish() {
		return kafkaSinkClosedError()
	}
	defer s.active.Done()

	msgs := make([]*sarama.ProducerMessage, 0, len(events))
	deliveries := make([]chan *sarama.ProducerError, 0, len(events))

	for _, event := range events {
		data, err := SerializeEvent(event, s.format)
		if err != nil {
			return fmt.Errorf("serializing event seq=%d: %w", event.GetLogSequence(), err)
		}

		eventType := strings.ToLower(event.GetType().String())
		delivery := make(chan *sarama.ProducerError, 1)
		msgs = append(msgs, &sarama.ProducerMessage{
			Topic:    s.topic,
			Key:      sarama.StringEncoder(event.GetLedger()),
			Value:    sarama.ByteEncoder(data),
			Metadata: delivery,
			Headers: []sarama.RecordHeader{
				{Key: []byte("event_type"), Value: []byte(eventType)},
			},
		})
		deliveries = append(deliveries, delivery)
	}

	if err := s.sendMessages(ctx, msgs); err != nil {
		return err
	}

	var producerErrs sarama.ProducerErrors
	for _, delivery := range deliveries {
		select {
		case producerErr := <-delivery:
			if producerErr != nil {
				producerErrs = append(producerErrs, producerErr)
			}
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closing:
			return kafkaSinkClosedError()
		}
	}

	if len(producerErrs) > 0 {
		return producerErrs
	}

	return nil
}

func (s *KafkaSink) Close() error {
	shouldClose := false

	s.closeMu.Lock()
	if !s.closed {
		s.closed = true
		shouldClose = true
		close(s.closing)
	}
	s.closeMu.Unlock()

	s.active.Wait()
	if shouldClose {
		s.producer.AsyncClose()
	}

	<-s.done

	return nil
}

func (s *KafkaSink) beginPublish() bool {
	s.closeMu.Lock()
	defer s.closeMu.Unlock()

	if s.closed {
		return false
	}

	s.active.Add(1)

	return true
}

func (s *KafkaSink) sendMessages(ctx context.Context, msgs []*sarama.ProducerMessage) error {
	for _, msg := range msgs {
		select {
		case s.producer.Input() <- msg:
		case <-ctx.Done():
			return ctx.Err()
		case <-s.closing:
			return kafkaSinkClosedError()
		}
	}

	return nil
}

func kafkaSinkClosedError() error {
	return errors.New("kafka sink is closed")
}

func (s *KafkaSink) dispatchDeliveries() {
	defer close(s.done)

	successes := s.producer.Successes()
	errorsCh := s.producer.Errors()

	for successes != nil || errorsCh != nil {
		select {
		case msg, ok := <-successes:
			if !ok {
				successes = nil

				continue
			}

			completeKafkaDelivery(msg, nil)
		case producerErr, ok := <-errorsCh:
			if !ok {
				errorsCh = nil

				continue
			}

			completeKafkaDelivery(producerErr.Msg, producerErr)
		}
	}
}

func completeKafkaDelivery(msg *sarama.ProducerMessage, producerErr *sarama.ProducerError) {
	if msg == nil {
		return
	}

	delivery, ok := msg.Metadata.(chan *sarama.ProducerError)
	if !ok {
		return
	}

	delivery <- producerErr
}

// configureSASL sets up SASL authentication on the sarama config if a mechanism is specified.
func configureSASL(saramaCfg *sarama.Config, cfg KafkaSinkConfig) error {
	if cfg.SASLMechanism == "" {
		return nil
	}

	saramaCfg.Net.SASL.Enable = true
	saramaCfg.Net.SASL.User = cfg.SASLUsername
	saramaCfg.Net.SASL.Password = cfg.SASLPassword

	switch strings.ToUpper(cfg.SASLMechanism) {
	case "PLAIN":
		saramaCfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case "SCRAM-SHA-256":
		saramaCfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		saramaCfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &scramClient{hashFcn: scram.SHA256}
		}
	case "SCRAM-SHA-512":
		saramaCfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		saramaCfg.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &scramClient{hashFcn: scram.SHA512}
		}
	default:
		return fmt.Errorf("unsupported SASL mechanism: %s (supported: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512)", cfg.SASLMechanism)
	}

	return nil
}

// scramClient implements sarama.SCRAMClient using xdg-go/scram.
type scramClient struct {
	hashFcn scram.HashGeneratorFcn
	conv    *scram.ClientConversation
}

func (s *scramClient) Begin(userName, password, authzID string) error {
	client, err := s.hashFcn.NewClient(userName, password, authzID)
	if err != nil {
		return fmt.Errorf("creating SCRAM client: %w", err)
	}

	s.conv = client.NewConversation()

	return nil
}

func (s *scramClient) Step(challenge string) (string, error) {
	return s.conv.Step(challenge)
}

func (s *scramClient) Done() bool {
	return s.conv.Done()
}
