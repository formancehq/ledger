//go:build kafka

package events

import (
	"context"
	"crypto/tls"
	"fmt"
	"strings"

	"github.com/IBM/sarama"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
	"github.com/xdg-go/scram"
)

func init() {
	registerSinkFactory("kafka", func(sc *commonpb.SinkConfig, format Format) (Sink, error) {
		s := sc.GetType().(*commonpb.SinkConfig_Kafka)
		return NewKafkaSink(KafkaSinkConfig{
			Brokers:       s.Kafka.Brokers,
			Topic:         s.Kafka.Topic,
			TLS:           s.Kafka.Tls,
			SASLMechanism: s.Kafka.SaslMechanism,
			SASLUsername:  s.Kafka.SaslUsername,
			SASLPassword:  s.Kafka.SaslPassword,
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
	SASLUsername   string
	SASLPassword  string
	Format        Format
}

// KafkaSink publishes events to Apache Kafka.
type KafkaSink struct {
	producer sarama.SyncProducer
	topic    string
	format   Format
}

// NewKafkaSink creates a new Kafka sink with a synchronous producer.
func NewKafkaSink(cfg KafkaSinkConfig) (*KafkaSink, error) {
	saramaCfg := sarama.NewConfig()
	saramaCfg.Producer.Return.Successes = true

	if cfg.TLS {
		saramaCfg.Net.TLS.Enable = true
		saramaCfg.Net.TLS.Config = &tls.Config{
			MinVersion: tls.VersionTLS12,
		}
	}

	if err := configureSASL(saramaCfg, cfg); err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducer(cfg.Brokers, saramaCfg)
	if err != nil {
		return nil, fmt.Errorf("creating Kafka producer: %w", err)
	}

	return &KafkaSink{
		producer: producer,
		topic:    cfg.Topic,
		format:   cfg.Format,
	}, nil
}

func (s *KafkaSink) Publish(_ context.Context, events []*eventspb.Event) error {
	msgs := make([]*sarama.ProducerMessage, 0, len(events))
	for _, event := range events {
		data, err := SerializeEvent(event, s.format)
		if err != nil {
			return fmt.Errorf("serializing event seq=%d: %w", event.LogSequence, err)
		}

		eventType := strings.ToLower(event.Type.String())
		msgs = append(msgs, &sarama.ProducerMessage{
			Topic: s.topic,
			Key:   sarama.StringEncoder(event.Ledger),
			Value: sarama.ByteEncoder(data),
			Headers: []sarama.RecordHeader{
				{Key: []byte("event_type"), Value: []byte(eventType)},
			},
		})
	}

	return s.producer.SendMessages(msgs)
}

func (s *KafkaSink) Close() error {
	return s.producer.Close()
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
