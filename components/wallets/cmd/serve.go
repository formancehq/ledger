package cmd

import (
	"fmt"

	sharedapi "github.com/formancehq/go-libs/api"
	"github.com/formancehq/go-libs/otlp/otlptraces"
	wallet "github.com/formancehq/wallets/pkg"
	"github.com/formancehq/wallets/pkg/api"
	"github.com/formancehq/wallets/pkg/client"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const (
	stackClientIDFlag     = "stack-client-id"
	stackClientSecretFlag = "stack-client-secret"
	stackURLFlag          = "stack-url"
	ledgerNameFlag        = "ledger"
	accountPrefixFlag     = "account-prefix"
)

var serveCmd = &cobra.Command{
	Use: "server",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		return bindFlagsToViper(cmd)
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		options := []fx.Option{
			fx.NopLogger,
			wallet.Module(viper.GetString(ledgerNameFlag), viper.GetString(accountPrefixFlag)),
			api.Module(sharedapi.ServiceInfo{
				Version: Version,
			}),
			client.NewModule(viper.GetString(stackClientIDFlag), viper.GetString(stackClientSecretFlag), viper.GetString(stackURLFlag)),
			otlptraces.CLITracesModule(viper.GetViper()),
		}

		app := fx.New(options...)
		if err := app.Start(cmd.Context()); err != nil {
			return fmt.Errorf("fx.App.Start: %w", err)
		}

		<-app.Done()

		if err := app.Stop(cmd.Context()); err != nil {
			return fmt.Errorf("fx.App.Stop: %w", err)
		}

		return nil
	},
}

func init() {
	serveCmd.Flags().String(stackClientIDFlag, "", "Client ID")
	serveCmd.Flags().String(stackClientSecretFlag, "", "Client Secret")
	serveCmd.Flags().String(stackURLFlag, "", "Token URL")
	serveCmd.Flags().String(ledgerNameFlag, "wallets-002", "Target ledger")
	serveCmd.Flags().String(accountPrefixFlag, "", "Account prefix flag")
	rootCmd.AddCommand(serveCmd)
}
