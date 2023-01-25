package kafka

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/webhooks/cmd/flag"
	"github.com/spf13/viper"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

type Client interface {
	PollFetches(ctx context.Context) kgo.Fetches
	PauseFetchTopics(topics ...string) []string
	ResumeFetchTopics(topics ...string)
	Close()
}

var ErrMechanism = errors.New("unrecognized SASL mechanism")

func NewClient() (*kgo.Client, []string, error) {
	logging.Infof("connecting to new kafka client...")
	var opts []kgo.Opt
	if viper.GetBool(flag.KafkaTLSEnabled) {
		opts = append(opts, kgo.DialTLSConfig(&tls.Config{
			MinVersion: tls.VersionTLS12,
		}))
	}

	if viper.GetBool(flag.KafkaSASLEnabled) {
		a := scram.Auth{
			User: viper.GetString(flag.KafkaUsername),
			Pass: viper.GetString(flag.KafkaPassword),
		}
		switch mechanism := viper.GetString(flag.KafkaSASLMechanism); mechanism {
		case "SCRAM-SHA-512":
			opts = append(opts, kgo.SASL(a.AsSha512Mechanism()))
		case "SCRAM-SHA-256":
			opts = append(opts, kgo.SASL(a.AsSha256Mechanism()))
		default:
			return nil, []string{}, ErrMechanism
		}
	}

	brokers := viper.GetStringSlice(flag.KafkaBrokers)
	opts = append(opts, kgo.SeedBrokers(brokers...))

	groupID := viper.GetString(flag.KafkaGroupID)
	opts = append(opts, kgo.ConsumerGroup(groupID))

	topics := viper.GetStringSlice(flag.KafkaTopics)
	opts = append(opts, kgo.ConsumeTopics(topics...))

	opts = append(opts, kgo.AllowAutoTopicCreation())

	kafkaClient, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, []string{}, fmt.Errorf("kgo.NewClient: %w", err)
	}

	healthy := false
	for !healthy {
		if err := kafkaClient.Ping(context.Background()); err != nil {
			logging.Infof("trying to reach broker: %s", err)
			time.Sleep(3 * time.Second)
		} else {
			healthy = true
		}
	}

	return kafkaClient, topics, nil
}
