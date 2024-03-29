package publish

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/stack/libs/go-libs/aws/iam"
	circuitbreaker "github.com/formancehq/stack/libs/go-libs/publish/circuit_breaker"
	topicmapper "github.com/formancehq/stack/libs/go-libs/publish/topic_mapper"
	"github.com/nats-io/nats.go"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/xdg-go/scram"
	"go.uber.org/fx"
)

const (
	// General configuration
	PublisherTopicMappingFlag = "publisher-topic-mapping"
	// Circuit Breaker configuration
	PublisherCircuitBreakerEnabledFlag              = "publisher-circuit-breaker-enabled"
	PublisherCircuitBreakerOpenIntervalDurationFlag = "publisher-circuit-breaker-open-interval-duration"
	PublisherCircuitBreakerSchemaFlag               = "publisher-circuit-breaker-schema"
	PublisherCircuitBreakerListStorageLimitFlag     = "publisher-circuit-breaker-list-storage-limit"
	// Kafka configuration
	PublisherKafkaEnabledFlag            = "publisher-kafka-enabled"
	PublisherKafkaBrokerFlag             = "publisher-kafka-broker"
	PublisherKafkaSASLEnabledFlag        = "publisher-kafka-sasl-enabled"
	PublisherKafkaSASLIAMEnabledFlag     = "publisher-kafka-sasl-iam-enabled"
	PublisherKafkaSASLIAMSessionNameFlag = "publisher-kafka-sasl-session-name"
	PublisherKafkaSASLUsernameFlag       = "publisher-kafka-sasl-username"
	PublisherKafkaSASLPasswordFlag       = "publisher-kafka-sasl-password"
	PublisherKafkaSASLMechanismFlag      = "publisher-kafka-sasl-mechanism"
	PublisherKafkaSASLScramSHASizeFlag   = "publisher-kafka-sasl-scram-sha-size"
	PublisherKafkaTLSEnabledFlag         = "publisher-kafka-tls-enabled"
	// HTTP configuration
	PublisherHttpEnabledFlag = "publisher-http-enabled"
	// Nats configuration
	PublisherNatsEnabledFlag       = "publisher-nats-enabled"
	PublisherNatsClientIDFlag      = "publisher-nats-client-id"
	PublisherNatsURLFlag           = "publisher-nats-url"
	PublisherNatsMaxReconnectFlag  = "publisher-nats-max-reconnect"
	PublisherNatsReconnectWaitFlag = "publisher-nats-reconnect-wait"
)

type ConfigDefault struct {
	PublisherTopicMapping []string
	// Circuit Breaker configuration
	PublisherCircuitBreakerEnabled              bool
	PublisherCircuitBreakerOpenIntervalDuration time.Duration
	PublisherCircuitBreakerSchema               string
	PublisherCircuitBreakerListStorageLimit     int
	// Kafka configuration
	PublisherKafkaEnabled            bool
	PublisherKafkaBroker             []string
	PublisherKafkaSASLEnabled        bool
	PublisherKafkaSASLIAMEnabled     bool
	PublisherKafkaSASLIAMSessionName string
	PublisherKafkaSASLUsername       string
	PublisherKafkaSASLPassword       string
	PublisherKafkaSASLMechanism      string
	PublisherKafkaSASLScramSHASize   int
	PublisherKafkaTLSEnabled         bool
	// HTTP configuration
	PublisherHttpEnabled bool
	// Nats configuration
	PublisherNatsEnabled       bool
	PublisherNatsClientID      string
	PublisherNatsURL           string
	PublisherNatsMaxReconnect  int
	PublisherNatsReconnectWait time.Duration
}

var (
	defaultConfigValues = ConfigDefault{
		PublisherTopicMapping:                       []string{},
		PublisherCircuitBreakerEnabled:              false,
		PublisherCircuitBreakerOpenIntervalDuration: 5 * time.Second,
		PublisherCircuitBreakerSchema:               "public",
		PublisherCircuitBreakerListStorageLimit:     100,
		PublisherKafkaEnabled:                       false,
		PublisherKafkaBroker:                        []string{"localhost:9092"},
		PublisherKafkaSASLEnabled:                   false,
		PublisherKafkaSASLIAMEnabled:                false,
		PublisherKafkaSASLIAMSessionName:            "",
		PublisherKafkaSASLUsername:                  "",
		PublisherKafkaSASLPassword:                  "",
		PublisherKafkaSASLMechanism:                 "",
		PublisherKafkaSASLScramSHASize:              512,
		PublisherKafkaTLSEnabled:                    false,
		PublisherHttpEnabled:                        false,
		PublisherNatsEnabled:                        false,
		PublisherNatsClientID:                       "",
		PublisherNatsURL:                            "",
		PublisherNatsMaxReconnect:                   -1, // We want to reconnect forever
		PublisherNatsReconnectWait:                  2 * time.Second,
	}
)

func InitCLIFlags(cmd *cobra.Command, options ...func(*ConfigDefault)) {
	values := defaultConfigValues
	for _, option := range options {
		option(&values)
	}

	// Circuit Breaker
	cmd.PersistentFlags().Bool(PublisherCircuitBreakerEnabledFlag, values.PublisherCircuitBreakerEnabled, "Enable circuit breaker for publisher")
	cmd.PersistentFlags().Duration(PublisherCircuitBreakerOpenIntervalDurationFlag, values.PublisherCircuitBreakerOpenIntervalDuration, "Circuit breaker open interval duration")
	cmd.PersistentFlags().String(PublisherCircuitBreakerSchemaFlag, values.PublisherCircuitBreakerSchema, "Circuit breaker schema")
	cmd.PersistentFlags().Int(PublisherCircuitBreakerListStorageLimitFlag, values.PublisherCircuitBreakerListStorageLimit, "Circuit breaker list storage limit")

	// HTTP
	cmd.PersistentFlags().Bool(PublisherHttpEnabledFlag, values.PublisherHttpEnabled, "Sent write event to http endpoint")

	// KAFKA
	cmd.PersistentFlags().Bool(PublisherKafkaEnabledFlag, values.PublisherKafkaEnabled, "Publish write events to kafka")
	cmd.PersistentFlags().StringSlice(PublisherKafkaBrokerFlag, values.PublisherKafkaBroker, "Kafka address is kafka enabled")
	cmd.PersistentFlags().StringSlice(PublisherTopicMappingFlag, values.PublisherTopicMapping, "Define mapping between internal event types and topics")
	cmd.PersistentFlags().Bool(PublisherKafkaSASLEnabledFlag, values.PublisherKafkaSASLEnabled, "Enable SASL authentication on kafka publisher")
	cmd.PersistentFlags().Bool(PublisherKafkaSASLIAMEnabledFlag, values.PublisherKafkaSASLIAMEnabled, "Enable IAM authentication on kafka publisher")
	cmd.PersistentFlags().String(PublisherKafkaSASLIAMSessionNameFlag, values.PublisherKafkaSASLIAMSessionName, "IAM session name")
	cmd.PersistentFlags().String(PublisherKafkaSASLUsernameFlag, values.PublisherKafkaSASLUsername, "SASL username")
	cmd.PersistentFlags().String(PublisherKafkaSASLPasswordFlag, values.PublisherKafkaSASLPassword, "SASL password")
	cmd.PersistentFlags().String(PublisherKafkaSASLMechanismFlag, values.PublisherKafkaSASLMechanism, "SASL authentication mechanism")
	cmd.PersistentFlags().Int(PublisherKafkaSASLScramSHASizeFlag, values.PublisherKafkaSASLScramSHASize, "SASL SCRAM SHA size")
	cmd.PersistentFlags().Bool(PublisherKafkaTLSEnabledFlag, values.PublisherKafkaTLSEnabled, "Enable TLS to connect on kafka")

	// NATS
	InitNatsCLIFlags(cmd, options...)
}

// DO NOT REMOVE: Used by membership
func InitNatsCLIFlags(cmd *cobra.Command, options ...func(*ConfigDefault)) {
	values := defaultConfigValues
	for _, option := range options {
		option(&values)
	}

	cmd.PersistentFlags().Bool(PublisherNatsEnabledFlag, values.PublisherNatsEnabled, "Publish write events to nats")
	cmd.PersistentFlags().String(PublisherNatsClientIDFlag, values.PublisherNatsClientID, "Nats client ID")
	cmd.PersistentFlags().Int(PublisherNatsMaxReconnectFlag, values.PublisherNatsMaxReconnect, "Nats: set the maximum number of reconnect attempts.")
	cmd.PersistentFlags().Duration(PublisherNatsReconnectWaitFlag, values.PublisherNatsReconnectWait, "Nats: the wait time between reconnect attempts.")
	cmd.PersistentFlags().String(PublisherNatsURLFlag, values.PublisherNatsURL, "Nats url")
}

func CLIPublisherModule(
	serviceName string,
) fx.Option {
	options := make([]fx.Option, 0)

	topics := viper.GetStringSlice(PublisherTopicMappingFlag)
	mapping := make(map[string]string)
	for _, topic := range topics {
		parts := strings.SplitN(topic, ":", 2)
		if len(parts) != 2 {
			panic(fmt.Sprintf("unable to parse topic '%s', must be two parts, separated by a colon", topic))
		}
		mapping[parts[0]] = parts[1]
	}

	options = append(options, Module(mapping))

	if viper.GetBool(PublisherCircuitBreakerEnabledFlag) {
		options = append(options,
			circuitbreaker.Module(
				viper.GetString(PublisherCircuitBreakerSchemaFlag),
				viper.GetDuration(PublisherCircuitBreakerOpenIntervalDurationFlag),
				viper.GetInt(PublisherCircuitBreakerListStorageLimitFlag),
			),
			fx.Decorate(func(cb *circuitbreaker.CircuitBreaker) message.Publisher {
				return cb
			}),
		)
	} else {
		options = append(options,
			fx.Decorate(func(topicMapper *topicmapper.TopicMapperPublisherDecorator) message.Publisher {
				return topicMapper
			}),
		)
	}

	switch {
	case viper.GetBool(PublisherHttpEnabledFlag):
		// Currently don't expose http listener, so pass addr == ""
		options = append(options, httpModule(""))
	case viper.GetBool(PublisherNatsEnabledFlag):
		options = append(options, NatsModule(
			viper.GetString(PublisherNatsURLFlag),
			serviceName,
			nats.Name(serviceName),
			nats.MaxReconnects(viper.GetInt(PublisherNatsMaxReconnectFlag)),
			nats.ReconnectWait(viper.GetDuration(PublisherNatsReconnectWaitFlag)),
		))
	case viper.GetBool(PublisherKafkaEnabledFlag):
		options = append(options,
			kafkaModule(clientId(serviceName), serviceName, viper.GetStringSlice(PublisherKafkaBrokerFlag)...),
			ProvideSaramaOption(
				WithConsumerReturnErrors(),
				WithProducerReturnSuccess(),
			),
		)
		if viper.GetBool(PublisherKafkaTLSEnabledFlag) {
			options = append(options, ProvideSaramaOption(WithTLS()))
		}
		if viper.GetBool(PublisherKafkaSASLEnabledFlag) {
			saramaOptions := []SaramaOption{
				WithSASLEnabled(),
				WithSASLMechanism(sarama.SASLMechanism(viper.GetString(PublisherKafkaSASLMechanismFlag))),
				WithSASLCredentials(
					viper.GetString(PublisherKafkaSASLUsernameFlag),
					viper.GetString(PublisherKafkaSASLPasswordFlag),
				),
				WithSASLScramClient(func() sarama.SCRAMClient {
					var fn scram.HashGeneratorFcn
					switch viper.GetInt(PublisherKafkaSASLScramSHASizeFlag) {
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
			}

			if viper.GetBool(PublisherKafkaSASLIAMEnabledFlag) {
				saramaOptions = append(saramaOptions,
					WithTokenProvider(&MSKAccessTokenProvider{
						region:      viper.GetString(iam.AWSRegionFlag),
						roleArn:     viper.GetString(iam.AWSRoleArnFlag),
						sessionName: viper.GetString(PublisherKafkaSASLIAMSessionNameFlag),
					}),
				)
			}

			options = append(options, ProvideSaramaOption(saramaOptions...))
		}
	default:
		options = append(options, GoChannelModule())
	}
	return fx.Options(options...)
}
