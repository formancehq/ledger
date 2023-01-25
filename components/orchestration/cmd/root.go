package cmd

import (
	"fmt"
	"os"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/logging/logginglogrus"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	ServiceName = "orchestration"
	Version     = "develop"
	BuildDate   = "-"
	Commit      = "-"
)

const (
	debugFlag                 = "debug"
	stackURLFlag              = "stack-url"
	stackClientIDFlag         = "stack-client-id"
	stackClientSecretFlag     = "stack-client-secret"
	temporalAddressFlag       = "temporal-address"
	temporalNamespaceFlag     = "temporal-namespace"
	temporalSSLClientKeyFlag  = "temporal-ssl-client-key"
	temporalSSLClientCertFlag = "temporal-ssl-client-cert"
	postgresDSNFlag           = "postgres-dsn"
)

var rootCmd = &cobra.Command{
	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {

		if err := bindFlagsToViper(cmd); err != nil {
			return err
		}

		logrusLogger := logrus.New()
		if viper.GetBool(debugFlag) {
			logrusLogger.SetLevel(logrus.DebugLevel)
			logrusLogger.Infof("Debug mode enabled.")
		}
		logger := logginglogrus.New(logrusLogger)
		logging.SetFactory(logging.StaticLoggerFactory(logger))

		return nil
	},
}

func exitWithCode(code int, v ...any) {
	fmt.Fprintln(os.Stdout, v...)
	os.Exit(code)
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		exitWithCode(1, err)
	}
}

func init() {
	rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
	rootCmd.PersistentFlags().BoolP(debugFlag, "d", false, "Debug mode")
	rootCmd.PersistentFlags().String(stackURLFlag, "", "Stack url")
	rootCmd.PersistentFlags().String(stackClientIDFlag, "", "Stack client ID")
	rootCmd.PersistentFlags().String(stackClientSecretFlag, "", "Stack client secret")
	rootCmd.PersistentFlags().String(temporalAddressFlag, "", "Temporal server address")
	rootCmd.PersistentFlags().String(temporalNamespaceFlag, "default", "Temporal namespace")
	rootCmd.PersistentFlags().String(temporalSSLClientKeyFlag, "", "Temporal client key")
	rootCmd.PersistentFlags().String(temporalSSLClientCertFlag, "", "Temporal client cert")
	rootCmd.PersistentFlags().String(postgresDSNFlag, "", "Postgres address")
}
