package api

import (
	_ "embed"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/health"
	"go.uber.org/fx"
)

type Config struct {
	Version string
}

func Module(cfg Config) fx.Option {
	return fx.Options(
		fx.Provide(routes.NewRouter),
		fx.Provide(func(storageDriver storage.Driver, resolver *ledger.Resolver) controllers.Backend {
			return controllers.NewDefaultBackend(storageDriver, cfg.Version, resolver)
		}),
		health.Module(),
	)
}
