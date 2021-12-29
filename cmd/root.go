package cmd

import (
	"fmt"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"os"
	"path"
	"strings"
)

const (
	debugFlag                           = "debug"
	storageDirFlag                      = "storage.dir"
	storageDriverFlag                   = "storage.driver"
	storageSQLiteDBNameFlag             = "storage.sqlite.db_name"
	storagePostgresConnectionStringFlag = "storage.postgres.conn_string"
	storageCacheFlag                    = "storage.cache"
	persistConfigFlag                   = "persist-config"
	serverHttpBindAddressFlag           = "server.http.bind_address"
	uiHttpBindAddressFlag               = "ui.http.bind_address"
	ledgersFlag                         = "ledgers"
	otelFlag                            = "otel"
	otelExporterFlag                    = "otel-exporter"
	serverHttpBasicAuthFlag             = "server.http.basic_auth"
	otelExporterJaegerEndpointFlag      = "otel-exporter-jaeger-endpoint"
	otelExporterJaegerUserFlag          = "otel-exporter-jaeger-user"
	otelExporterJaegerPasswordFlag      = "otel-exporter-jaeger-password"
	otelExporterOTLPModeFlag            = "otel-exporter-otlp-mode"
	otelExporterOTLPEndpointFlag        = "otel-exporter-otlp-endpoint"
	otelExporterOTLPInsecureFlag        = "otel-exporter-otlp-insecure"
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
	root.PersistentFlags().Bool(otelFlag, false, "Enable OpenTelemetry support")
	root.PersistentFlags().String(otelExporterFlag, "stdout", "OpenTelemetry exporter")
	root.PersistentFlags().String(otelExporterJaegerEndpointFlag, "", "Jaeger exporter endpoint")
	root.PersistentFlags().String(otelExporterJaegerUserFlag, "", "Jaeger exporter user")
	root.PersistentFlags().String(otelExporterJaegerPasswordFlag, "", "Jaeger exporter password")
	root.PersistentFlags().String(serverHttpBasicAuthFlag, "", "Http basic auth")
	root.PersistentFlags().String(otelExporterOTLPModeFlag, "grpc", "OpenTelemetry OTLP exporter mode (grpc|http)")
	root.PersistentFlags().String(otelExporterOTLPEndpointFlag, "", "OpenTelemetry grpc endpoint")
	root.PersistentFlags().Bool(otelExporterOTLPInsecureFlag, false, "OpenTelemetry grpc insecure")

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
