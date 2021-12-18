package cmd

import (
	"context"
	"github.com/numary/ledger/api"
	"github.com/numary/ledger/api/controllers"
	"github.com/numary/ledger/ledger"
	"github.com/numary/ledger/storage"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func NewContainer(options ...fx.Option) *fx.App {
	providers := make([]interface{}, 0)
	providers = append(providers,
		fx.Annotate(func() string { return viper.GetString("version") }, fx.ResultTags(`name:"version"`)),
		fx.Annotate(func() string { return viper.GetString("storage.driver") }, fx.ResultTags(`name:"storageDriver"`)),
		fx.Annotate(func() controllers.LedgerLister {
			return controllers.LedgerListerFn(func() []string {
				// Ledgers are updated by function config.Remember
				// We have to resolve the list dynamically
				return viper.GetStringSlice("ledgers")
			})
		}, fx.ResultTags(`name:"ledgerLister"`)),
		fx.Annotate(func() string { return viper.GetString("server.http.basic_auth") }, fx.ResultTags(`name:"httpBasic"`)),
	)
	if viper.GetBool("storage.cache") {
		providers = append(providers, fx.Annotate(
			storage.NewDefaultFactory,
			fx.ParamTags(`name:"storageDriver"`),
			fx.ResultTags(`name:"underlyingStorage"`),
		))
		providers = append(providers, fx.Annotate(
			storage.NewCachedStorageFactory,
			fx.ParamTags(`name:"underlyingStorage"`),
			fx.As(new(storage.Factory)),
		))
		providers = append(providers, fx.Annotate(
			ledger.WithStorageFactory,
			fx.ResultTags(`group:"resolverOptions"`),
			fx.As(new(ledger.ResolverOption)),
		))
	} else {
		providers = append(providers, fx.Annotate(
			storage.NewDefaultFactory,
			fx.ParamTags(`name:"storageDriver"`),
		))
	}
	providers = append(providers,
		fx.Annotate(ledger.NewResolver, fx.ParamTags(`group:"resolverOptions"`)),
		api.NewAPI,
	)

	options = append(
		[]fx.Option{
			fx.Invoke(func(lc fx.Lifecycle, h *api.API, storageFactory storage.Factory) {
				lc.Append(fx.Hook{
					OnStop: func(ctx context.Context) error {
						err := storageFactory.Close(ctx)
						if err != nil {
							return errors.Wrap(err, "closing storage factory")
						}
						return nil
					},
				})
			}),
			fx.Provide(providers...),
			api.Module,
		},
		options...,
	)

	return fx.New(options...)
}
