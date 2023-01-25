package cmd

import (
	"fmt"
	"os"

	sharedlogging "github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/logging/logginglogrus"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var (
	ServiceName = "wallets"
	Version     = "develop"
	BuildDate   = "-"
	Commit      = "-"
)

const (
	debugFlag = "debug"
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
		sharedlogging.SetFactory(sharedlogging.StaticLoggerFactory(logger))

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
}
