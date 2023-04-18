package publish

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/xdg-go/scram"
	"go.uber.org/fx"
)

const (
	// General configuration
	PublisherTopicMappingFlag = "publisher-topic-mapping"
	// Kafka configuration
	PublisherKafkaEnabledFlag      = "publisher-kafka-enabled"
	PublisherKafkaBrokerFlag       = "publisher-kafka-broker"
	PublisherKafkaSASLEnabled      = "publisher-kafka-sasl-enabled"
	PublisherKafkaSASLUsername     = "publisher-kafka-sasl-username"
	PublisherKafkaSASLPassword     = "publisher-kafka-sasl-password"
	PublisherKafkaSASLMechanism    = "publisher-kafka-sasl-mechanism"
	PublisherKafkaSASLScramSHASize = "publisher-kafka-sasl-scram-sha-size"
	PublisherKafkaTLSEnabled       = "publisher-kafka-tls-enabled"
	// HTTP configuration
	PublisherHttpEnabledFlag = "publisher-http-enabled"
	// Nats configuration
	PublisherNatsEnabledFlag  = "publisher-nats-enabled"
	PublisherNatsClientIDFlag = "publisher-nats-client-id"
	PublisherNatsURLFlag      = "publisher-nats-url"
)

func InitCLIFlags(cmd *cobra.Command) {
	cmd.PersistentFlags().Bool(PublisherKafkaEnabledFlag, false, "Publish write events to kafka")
	cmd.PersistentFlags().StringSlice(PublisherKafkaBrokerFlag, []string{"localhost:9092"}, "Kafka address is kafka enabled")
	cmd.PersistentFlags().StringSlice(PublisherTopicMappingFlag, []string{}, "Define mapping between internal event types and topics")
	cmd.PersistentFlags().Bool(PublisherHttpEnabledFlag, false, "Sent write event to http endpoint")
	cmd.PersistentFlags().Bool(PublisherKafkaSASLEnabled, false, "Enable SASL authentication on kafka publisher")
	cmd.PersistentFlags().String(PublisherKafkaSASLUsername, "", "SASL username")
	cmd.PersistentFlags().String(PublisherKafkaSASLPassword, "", "SASL password")
	cmd.PersistentFlags().String(PublisherKafkaSASLMechanism, "", "SASL authentication mechanism")
	cmd.PersistentFlags().Int(PublisherKafkaSASLScramSHASize, 512, "SASL SCRAM SHA size")
	cmd.PersistentFlags().Bool(PublisherKafkaTLSEnabled, false, "Enable TLS to connect on kafka")
	cmd.PersistentFlags().Bool(PublisherNatsEnabledFlag, false, "Publish write events to nats")
	cmd.PersistentFlags().String(PublisherNatsClientIDFlag, "", "Nats client ID")
	cmd.PersistentFlags().String(PublisherNatsURLFlag, "", "Nats url")
}

func CLIPublisherModule(v *viper.Viper, serviceName string) fx.Option {
	options := make([]fx.Option, 0)

	topics := v.GetStringSlice(PublisherTopicMappingFlag)
	mapping := make(map[string]string)
	for _, topic := range topics {
		parts := strings.SplitN(topic, ":", 2)
		if len(parts) != 2 {
			panic(fmt.Sprintf("unable to parse topic '%s', must be two parts, separated by a colon", topic))
		}
		mapping[parts[0]] = parts[1]
	}

	options = append(options, Module(mapping))
	switch {
	case v.GetBool(PublisherHttpEnabledFlag):
		// Currently don't expose http listener, so pass addr == ""
		options = append(options, httpModule(""))
	case v.GetBool(PublisherNatsEnabledFlag):
		options = append(options, natsModule(
			v.GetString(PublisherNatsClientIDFlag), v.GetString(PublisherNatsURLFlag), serviceName))
	case v.GetBool(PublisherKafkaEnabledFlag):
		options = append(options,
			kafkaModule(clientId(serviceName), serviceName, v.GetStringSlice(PublisherKafkaBrokerFlag)...),
			ProvideSaramaOption(
				WithConsumerReturnErrors(),
				WithProducerReturnSuccess(),
			),
		)
		if v.GetBool(PublisherKafkaTLSEnabled) {
			options = append(options, ProvideSaramaOption(WithTLS()))
		}
		if v.GetBool(PublisherKafkaSASLEnabled) {
			options = append(options, ProvideSaramaOption(
				WithSASLEnabled(),
				WithSASLCredentials(
					v.GetString(PublisherKafkaSASLUsername),
					v.GetString(PublisherKafkaSASLPassword),
				),
				WithSASLMechanism(sarama.SASLMechanism(v.GetString(PublisherKafkaSASLMechanism))),
				WithSASLScramClient(func() sarama.SCRAMClient {
					var fn scram.HashGeneratorFcn
					switch v.GetInt(PublisherKafkaSASLScramSHASize) {
					case 512:
						fn = SHA512
					case 256:
						fn = SHA256
					default:
						panic("sha size not handled")
					}
					return &XDGSCRAMClient{
						HashGeneratorFcn: fn,
					}
				}),
			))
		}
	default:
		options = append(options, GoChannelModule())
	}
	return fx.Options(options...)
}
