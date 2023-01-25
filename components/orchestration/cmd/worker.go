package cmd

import (
	"context"
	"fmt"
	"net/http"

	"github.com/formancehq/formance-sdk-go"
	"github.com/formancehq/go-libs/otlp/otlptraces"
	"github.com/formancehq/orchestration/internal/activities"
	"github.com/formancehq/orchestration/internal/storage"
	"github.com/formancehq/orchestration/internal/temporal"
	"github.com/formancehq/orchestration/internal/workflow"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
	"go.uber.org/fx"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/clientcredentials"
)

func temporalWorkerModule() fx.Option {
	return fx.Options(
		temporal.NewModule(
			viper.GetString(temporalAddressFlag),
			viper.GetString(temporalNamespaceFlag),
			viper.GetString(temporalSSLClientCertFlag),
			viper.GetString(temporalSSLClientKeyFlag),
		),
		fx.Invoke(func(lc fx.Lifecycle, c client.Client, apiClient *formance.APIClient, db *bun.DB) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					w := worker.New(c, workflow.TaskQueueName, worker.Options{})

					workflow := workflow.NewRunner(db)
					activities := activities.New(apiClient)
					w.RegisterWorkflow(workflow.Run)
					w.RegisterActivity(activities.VoidHold)
					w.RegisterActivity(activities.ConfirmHold)
					w.RegisterActivity(activities.StripeTransfer)
					w.RegisterActivity(activities.GetWallet)
					w.RegisterActivity(activities.DebitWallet)
					w.RegisterActivity(activities.GetPayment)
					w.RegisterActivity(activities.CreditWallet)
					w.RegisterActivity(activities.CreateTransaction)
					w.RegisterActivity(activities.RevertTransaction)

					go func() {
						err := w.Run(worker.InterruptCh())
						if err != nil {
							panic(err)
						}
					}()
					return nil
				},
			})
		}),
	)
}

func stackClientModule() fx.Option {
	return fx.Options(
		fx.Provide(func() *formance.APIClient {
			configuration := formance.NewConfiguration()
			configuration.Servers = []formance.ServerConfiguration{{
				URL: viper.GetString(stackURLFlag),
			}}
			configuration.Debug = viper.GetBool(debugFlag)
			oauthConfig := clientcredentials.Config{
				ClientID:     viper.GetString(stackClientIDFlag),
				ClientSecret: viper.GetString(stackClientSecretFlag),
				TokenURL:     fmt.Sprintf("%s/api/auth/oauth/token", viper.GetString(stackURLFlag)),
			}
			underlyingHTTPClient := &http.Client{
				Transport: otelhttp.NewTransport(http.DefaultTransport),
			}
			configuration.HTTPClient = oauthConfig.Client(context.WithValue(context.Background(),
				oauth2.HTTPClient, underlyingHTTPClient))
			return formance.NewAPIClient(configuration)
		}),
	)
}

var workerCommand = &cobra.Command{
	Use: "worker",
	PreRunE: func(cmd *cobra.Command, args []string) error {
		return bindFlagsToViper(cmd)
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		options := []fx.Option{
			fx.NopLogger,
			otlptraces.CLITracesModule(viper.GetViper()),
			storage.NewModule(viper.GetString(postgresDSNFlag), viper.GetBool(debugFlag)),
			stackClientModule(),
			temporalWorkerModule(),
		}

		app := fx.New(options...)
		err := app.Start(cmd.Context())
		if err != nil {
			return err
		}
		<-app.Done()
		return app.Err()
	},
}

func init() {
	rootCmd.AddCommand(workerCommand)
}
