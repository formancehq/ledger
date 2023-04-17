package cmd

import (
	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/stack/libs/go-libs/ballast"
	"github.com/formancehq/stack/libs/go-libs/httpserver"
	app "github.com/formancehq/stack/libs/go-libs/service"
	"github.com/go-chi/chi/v5"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const (
	cacheEvictionPeriodFlag  = "cache-eviction-period"
	cacheEvictionRetainDelay = "cache-eviction-retain-delay"
	queryLimitReadLogsFlag   = "query-limit-read-logs"

	ballastSizeInBytesFlag = "ballast-size"
)

func NewServe() *cobra.Command {
	cmd := &cobra.Command{
		Use: "serve",
		RunE: func(cmd *cobra.Command, args []string) error {
			return app.New(cmd.OutOrStdout(), resolveOptions(
				viper.GetViper(),
				ballast.Module(viper.GetUint(ballastSizeInBytesFlag)),
				fx.Invoke(func(lc fx.Lifecycle, h chi.Router) {
					lc.Append(httpserver.NewHook(viper.GetString(bindFlag), h))
				}),
			)...).Run(cmd.Context())
		},
	}
	cmd.Flags().Duration(cacheEvictionPeriodFlag, cache.DefaultEvictionPeriod, "Cache eviction period")
	cmd.Flags().Duration(cacheEvictionRetainDelay, cache.DefaultRetainDelay, "Cache retain delay")
	cmd.Flags().Int(queryLimitReadLogsFlag, 10000, "Query limit read logs")
	cmd.Flags().Uint(ballastSizeInBytesFlag, 0, "Ballast size in bytes, default to 0")
	return cmd
}
