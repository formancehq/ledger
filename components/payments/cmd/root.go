//nolint:gochecknoglobals,golint,revive // allow for cobra & logrus init
package cmd

import (
	"fmt"
	"os"
	"strings"

	_ "github.com/bombsimon/logrusr/v3"
	"github.com/formancehq/go-libs/otlp/otlptraces"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	debugFlag = "debug"
)

var (
	Version   = "develop"
	BuildDate = "-"
	Commit    = "-"
)

func rootCommand() *cobra.Command {
	viper.SetDefault("version", Version)

	root := &cobra.Command{
		Use:               "payments",
		Short:             "payments",
		DisableAutoGenTag: true,
	}

	version := newVersion()
	root.AddCommand(version)

	server := newServer()
	root.AddCommand(newServer())

	migrate := newMigrate()
	root.AddCommand(migrate)

	root.PersistentFlags().Bool(debugFlag, false, "Debug mode")

	migrate.Flags().String(postgresURIFlag, "postgres://localhost/payments", "PostgreSQL DB address")

	server.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	server.Flags().String(postgresURIFlag, "postgres://localhost/payments", "PostgreSQL DB address")
	server.Flags().String(envFlag, "local", "Environment")
	server.Flags().Bool(publisherKafkaEnabledFlag, false, "Publish write events to kafka")
	server.Flags().StringSlice(publisherKafkaBrokerFlag, []string{}, "Kafka address is kafka enabled")
	server.Flags().StringSlice(publisherTopicMappingFlag,
		[]string{}, "Define mapping between internal event types and topics")
	server.Flags().Bool(publisherHTTPEnabledFlag, false, "Sent write event to http endpoint")
	server.Flags().Bool(publisherKafkaSASLEnabled, false, "Enable SASL authentication on kafka publisher")
	server.Flags().String(publisherKafkaSASLUsername, "", "SASL username")
	server.Flags().String(publisherKafkaSASLPassword, "", "SASL password")
	server.Flags().String(publisherKafkaSASLMechanism, "", "SASL authentication mechanism")
	server.Flags().Int(publisherKafkaSASLScramSHASize, 512, "SASL SCRAM SHA size")
	server.Flags().Bool(publisherKafkaTLSEnabled, false, "Enable TLS to connect on kafka")
	server.Flags().Bool(authBasicEnabledFlag, false, "Enable basic auth")
	server.Flags().StringSlice(authBasicCredentialsFlag, []string{},
		"HTTP basic auth credentials (<username>:<password>)")
	server.Flags().Bool(authBearerEnabledFlag, false, "Enable bearer auth")
	server.Flags().String(authBearerIntrospectURLFlag, "", "OAuth2 introspect URL")
	server.Flags().StringSlice(authBearerAudienceFlag, []string{}, "Allowed audiences")
	server.Flags().Bool(authBearerAudiencesWildcardFlag, false, "Don't check audience")
	server.Flags().Bool(authBearerUseScopesFlag,
		false, "Use scopes as defined by rfc https://datatracker.ietf.org/doc/html/rfc8693")

	otlptraces.InitOTLPTracesFlags(server.Flags())

	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	viper.AutomaticEnv()

	err := viper.BindPFlags(root.Flags())
	if err != nil {
		panic(err)
	}

	return root
}

func Execute() {
	if err := rootCommand().Execute(); err != nil {
		if _, err = fmt.Fprintln(os.Stderr, err); err != nil {
			panic(err)
		}

		os.Exit(1)
	}
}
