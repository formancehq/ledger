package cmd

import (
	"strings"

	"github.com/formancehq/go-libs/otlp/otlptraces"

	"github.com/bombsimon/logrusr/v3"
	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/payments/internal/app/api"
	"github.com/formancehq/payments/internal/app/storage"
	"github.com/pkg/errors"
	"go.opentelemetry.io/otel"

	"github.com/Shopify/sarama"
	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/logging/logginglogrus"
	"github.com/formancehq/go-libs/publish"
	"github.com/formancehq/go-libs/publish/publishhttp"
	"github.com/formancehq/go-libs/publish/publishkafka"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/opentelemetry-go-extra/otellogrus"
	"github.com/xdg-go/scram"
	"go.uber.org/fx"
)

//nolint:gosec // false positive
const (
	postgresURIFlag                 = "postgres-uri"
	otelTracesFlag                  = "otel-traces"
	envFlag                         = "env"
	publisherKafkaEnabledFlag       = "publisher-kafka-enabled"
	publisherKafkaBrokerFlag        = "publisher-kafka-broker"
	publisherKafkaSASLEnabled       = "publisher-kafka-sasl-enabled"
	publisherKafkaSASLUsername      = "publisher-kafka-sasl-username"
	publisherKafkaSASLPassword      = "publisher-kafka-sasl-password"
	publisherKafkaSASLMechanism     = "publisher-kafka-sasl-mechanism"
	publisherKafkaSASLScramSHASize  = "publisher-kafka-sasl-scram-sha-size"
	publisherKafkaTLSEnabled        = "publisher-kafka-tls-enabled"
	publisherTopicMappingFlag       = "publisher-topic-mapping"
	publisherHTTPEnabledFlag        = "publisher-http-enabled"
	authBasicEnabledFlag            = "auth-basic-enabled"
	authBasicCredentialsFlag        = "auth-basic-credentials"
	authBearerEnabledFlag           = "auth-bearer-enabled"
	authBearerIntrospectURLFlag     = "auth-bearer-introspect-url"
	authBearerAudienceFlag          = "auth-bearer-audience"
	authBearerAudiencesWildcardFlag = "auth-bearer-audiences-wildcard"
	authBearerUseScopesFlag         = "auth-bearer-use-scopes"

	serviceName = "Payments"
)

func newServer() *cobra.Command {
	return &cobra.Command{
		Use:          "server",
		Short:        "Launch server",
		SilenceUsage: true,
		RunE:         runServer,
	}
}

func runServer(cmd *cobra.Command, args []string) error {
	setLogger()

	databaseOptions, err := prepareDatabaseOptions()
	if err != nil {
		return err
	}

	options := make([]fx.Option, 0)

	if !viper.GetBool(debugFlag) {
		options = append(options, fx.NopLogger)
	}

	options = append(options, databaseOptions)
	options = append(options, otlptraces.CLITracesModule(viper.GetViper()))

	options = append(options,
		fx.Provide(fx.Annotate(func(p message.Publisher) *publish.TopicMapperPublisher {
			return publish.NewTopicMapperPublisher(p, topicsMapping())
		}, fx.As(new(publish.Publisher)))))

	options = append(options, api.HTTPModule(sharedapi.ServiceInfo{
		Version: Version,
	}))
	options = append(options, publish.Module())

	switch {
	case viper.GetBool(publisherHTTPEnabledFlag):
		options = append(options, publishhttp.Module())
	case viper.GetBool(publisherKafkaEnabledFlag):
		options = append(options,
			publishkafka.Module(serviceName, viper.GetStringSlice(publisherKafkaBrokerFlag)...),
			publishkafka.ProvideSaramaOption(
				publishkafka.WithConsumerReturnErrors(),
				publishkafka.WithProducerReturnSuccess(),
			),
		)

		if viper.GetBool(publisherKafkaTLSEnabled) {
			options = append(options, publishkafka.ProvideSaramaOption(publishkafka.WithTLS()))
		}

		if viper.GetBool(publisherKafkaSASLEnabled) {
			options = append(options, publishkafka.ProvideSaramaOption(
				publishkafka.WithSASLEnabled(),
				publishkafka.WithSASLCredentials(
					viper.GetString(publisherKafkaSASLUsername),
					viper.GetString(publisherKafkaSASLPassword),
				),
				publishkafka.WithSASLMechanism(sarama.SASLMechanism(viper.GetString(publisherKafkaSASLMechanism))),
				publishkafka.WithSASLScramClient(setSCRAMClient),
			))
		}
	}

	err = fx.New(options...).Start(cmd.Context())
	if err != nil {
		return err
	}

	<-cmd.Context().Done()

	return nil
}

func setLogger() {
	log := logrus.New()

	if viper.GetBool(debugFlag) {
		log.SetLevel(logrus.DebugLevel)
	}

	if viper.GetBool(otelTracesFlag) {
		log.AddHook(otellogrus.NewHook(otellogrus.WithLevels(
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
		)))
		log.SetFormatter(&logrus.JSONFormatter{})
	}

	logging.SetFactory(logging.StaticLoggerFactory(logginglogrus.New(log)))

	// Add a dedicated logger for opentelemetry in case of error
	otel.SetLogger(logrusr.New(logrus.New().WithField("component", "otlp")))
}

func prepareDatabaseOptions() (fx.Option, error) {
	postgresURI := viper.GetString(postgresURIFlag)
	if postgresURI == "" {
		return nil, errors.New("missing postgres uri")
	}

	return storage.Module(postgresURI), nil
}

func topicsMapping() map[string]string {
	topics := viper.GetStringSlice(publisherTopicMappingFlag)
	mapping := make(map[string]string)

	for _, topic := range topics {
		parts := strings.SplitN(topic, ":", 2)
		if len(parts) != 2 {
			panic("invalid topic flag")
		}

		mapping[parts[0]] = parts[1]
	}

	return mapping
}

func setSCRAMClient() sarama.SCRAMClient {
	var fn scram.HashGeneratorFcn

	switch viper.GetInt(publisherKafkaSASLScramSHASize) {
	case 512:
		fn = publishkafka.SHA512
	case 256:
		fn = publishkafka.SHA256
	default:
		panic("sha size not handled")
	}

	return &publishkafka.XDGSCRAMClient{
		HashGeneratorFcn: fn,
	}
}
