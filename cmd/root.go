package cmd

import (
	"fmt"
	"github.com/numary/ledger/pkg/redis"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"path"
	"strings"
)

const (
	debugFlag                            = "debug"
	storageDirFlag                       = "storage.dir"
	storageDriverFlag                    = "storage.driver"
	storageSQLiteDBNameFlag              = "storage.sqlite.db_name"
	storagePostgresConnectionStringFlag  = "storage.postgres.conn_string"
	storageCacheFlag                     = "storage.cache"
	persistConfigFlag                    = "persist-config"
	serverHttpBindAddressFlag            = "server.http.bind_address"
	uiHttpBindAddressFlag                = "ui.http.bind_address"
	ledgersFlag                          = "ledgers"
	serverHttpBasicAuthFlag              = "server.http.basic_auth"
	lockStrategyFlag                     = "lock-strategy"
	lockStrategyRedisUrlFlag             = "lock-strategy-redis-url"
	lockStrategyRedisDurationFlag        = "lock-strategy-redis-duration"
	lockStrategyRedisRetryFlag           = "lock-strategy-redis-retry"
	lockStrategyRedisTLSEnabledFlag      = "lock-strategy-redis-tls-enabled"
	lockStrategyRedisTLSInsecureFlag     = "lock-strategy-redis-tls-insecure"
	otelTracesFlag                       = "otel-traces"
	otelTracesBatchFlag                  = "otel-traces-batch"
	otelTracesExporterFlag               = "otel-traces-exporter"
	otelTracesExporterJaegerEndpointFlag = "otel-traces-exporter-jaeger-endpoint"
	otelTracesExporterJaegerUserFlag     = "otel-traces-exporter-jaeger-user"
	otelTracesExporterJaegerPasswordFlag = "otel-traces-exporter-jaeger-password"
	otelTracesExporterOTLPModeFlag       = "otel-traces-exporter-otlp-mode"
	otelTracesExporterOTLPEndpointFlag   = "otel-traces-exporter-otlp-endpoint"
	otelTracesExporterOTLPInsecureFlag   = "otel-traces-exporter-otlp-insecure"
	otelMetricsFlag                      = "otel-metrics"
	otelMetricsExporterFlag              = "otel-metrics-exporter"
	otelMetricsExporterOTLPModeFlag      = "otel-metrics-exporter-otlp-mode"
	otelMetricsExporterOTLPEndpointFlag  = "otel-metrics-exporter-otlp-endpoint"
	otelMetricsExporterOTLPInsecureFlag  = "otel-metrics-exporter-otlp-insecure"
	publisherKafkaEnabledFlag            = "publisher-kafka-enabled"
	publisherBusKafkaBrokerFlag          = "publisher-kafka-broker"
	publisherTopicMappingFlag            = "publisher-topic-mapping"
	publisherHttpEnabledFlag             = "publisher-http-enabled"
)

var (
	Version   = "develop"
	BuildDate = "-"
	Commit    = "-"
)

func NewRootCommand() *cobra.Command {
	viper.SetDefault("version", Version)

	root := &cobra.Command{
		Use:               "numary",
		Short:             "Numary",
		DisableAutoGenTag: true,
		PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
			err := os.MkdirAll(viper.GetString(storageDirFlag), 0700)
			if err != nil {
				return errors.Wrap(err, "creating storage directory")
			}

			if viper.GetBool(debugFlag) {
				logrus.StandardLogger().Level = logrus.DebugLevel
			}
			return nil
		},
	}

	server := NewServer()
	version := NewVersion()
	start := NewServerStart()
	server.AddCommand(start)

	conf := NewConfig()
	conf.AddCommand(NewConfigInit())
	store := NewStorage()
	store.AddCommand(NewStorageInit())

	scriptExec := NewScriptExec()
	scriptCheck := NewScriptCheck()

	root.AddCommand(server)
	root.AddCommand(conf)
	root.AddCommand(UICmd)
	root.AddCommand(store)
	root.AddCommand(scriptExec)
	root.AddCommand(scriptCheck)
	root.AddCommand(version)
	root.AddCommand(stickersCmd)

	home, err := os.UserHomeDir()
	if err != nil {
		home = "/root"
	}

	root.PersistentFlags().Bool(debugFlag, false, "Debug mode")
	root.PersistentFlags().String(storageDriverFlag, "sqlite", "Storage driver")
	root.PersistentFlags().String(storageDirFlag, path.Join(home, ".numary/data"), "Storage directory (for sqlite)")
	root.PersistentFlags().String(storageSQLiteDBNameFlag, "numary", "SQLite database name")
	root.PersistentFlags().String(storagePostgresConnectionStringFlag, "postgresql://localhost/postgres", "Postgre connection string")
	root.PersistentFlags().Bool(storageCacheFlag, true, "Storage cache")
	root.PersistentFlags().Bool(persistConfigFlag, true, "Persist config on disk")
	root.PersistentFlags().String(serverHttpBindAddressFlag, "localhost:3068", "API bind address")
	root.PersistentFlags().String(uiHttpBindAddressFlag, "localhost:3068", "UI bind address")
	root.PersistentFlags().StringSlice(ledgersFlag, []string{"quickstart"}, "Ledgers")
	root.PersistentFlags().String(serverHttpBasicAuthFlag, "", "Http basic auth")
	root.PersistentFlags().Bool(otelTracesFlag, false, "Enable OpenTelemetry traces support")
	root.PersistentFlags().Bool(otelTracesBatchFlag, false, "Use OpenTelemetry batching")
	root.PersistentFlags().String(otelTracesExporterFlag, "stdout", "OpenTelemetry traces exporter")
	root.PersistentFlags().String(otelTracesExporterJaegerEndpointFlag, "", "OpenTelemetry traces Jaeger exporter endpoint")
	root.PersistentFlags().String(otelTracesExporterJaegerUserFlag, "", "OpenTelemetry traces Jaeger exporter user")
	root.PersistentFlags().String(otelTracesExporterJaegerPasswordFlag, "", "OpenTelemetry traces Jaeger exporter password")
	root.PersistentFlags().String(otelTracesExporterOTLPModeFlag, "grpc", "OpenTelemetry traces OTLP exporter mode (grpc|http)")
	root.PersistentFlags().String(otelTracesExporterOTLPEndpointFlag, "", "OpenTelemetry traces grpc endpoint")
	root.PersistentFlags().Bool(otelTracesExporterOTLPInsecureFlag, false, "OpenTelemetry traces grpc insecure")
	root.PersistentFlags().Bool(otelMetricsFlag, false, "Enable OpenTelemetry metrics support")
	root.PersistentFlags().String(otelMetricsExporterFlag, "stdout", "OpenTelemetry metrics exporter")
	root.PersistentFlags().String(otelMetricsExporterOTLPModeFlag, "grpc", "OpenTelemetry metrics OTLP exporter mode (grpc|http)")
	root.PersistentFlags().String(otelMetricsExporterOTLPEndpointFlag, "", "OpenTelemetry metrics grpc endpoint")
	root.PersistentFlags().Bool(otelMetricsExporterOTLPInsecureFlag, false, "OpenTelemetry metrics grpc insecure")
	root.PersistentFlags().String(lockStrategyFlag, "memory", "Lock strategy (memory, none, redis)")
	root.PersistentFlags().String(lockStrategyRedisUrlFlag, "", "Redis url when using redis locking strategy")
	root.PersistentFlags().Duration(lockStrategyRedisDurationFlag, redis.DefaultLockDuration, "Lock duration")
	root.PersistentFlags().Duration(lockStrategyRedisRetryFlag, redis.DefaultRetryInterval, "Retry lock period")
	root.PersistentFlags().Bool(lockStrategyRedisTLSEnabledFlag, false, "Use tls on redis")
	root.PersistentFlags().Bool(lockStrategyRedisTLSInsecureFlag, false, "Whether redis is trusted or not")
	root.PersistentFlags().Bool(publisherKafkaEnabledFlag, false, "Publish write events to kafka")
	root.PersistentFlags().StringSlice(publisherBusKafkaBrokerFlag, []string{}, "Kafka address is kafka enabled")
	root.PersistentFlags().StringSlice(publisherTopicMappingFlag, []string{}, "Define mapping between internal event types and topics")
	root.PersistentFlags().Bool(publisherHttpEnabledFlag, false, "Sent write event to http endpoint")

	viper.BindPFlags(root.PersistentFlags())
	viper.SetConfigName("numary")
	viper.SetConfigType("yaml")
	viper.AddConfigPath("$HOME/.numary")
	viper.AddConfigPath("/etc/numary")
	viper.ReadInConfig()

	viper.SetEnvPrefix("numary")
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_", "-", "_"))
	viper.AutomaticEnv()

	return root
}

func Execute() {
	if err := NewRootCommand().Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
