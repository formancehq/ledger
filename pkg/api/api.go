package api

import (
	_ "embed"

	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/stack/libs/go-libs/health"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/go-chi/chi/v5"
	"go.uber.org/fx"
)

type Config struct {
	Version string
}

func Module(cfg Config) fx.Option {
	return fx.Options(
		fx.Provide(func(storageDriver storage.Driver, resolver *ledger.Resolver, logger logging.Logger,
			healthController *health.HealthController) chi.Router {
			return routes.NewRouter(storageDriver, cfg.Version, resolver, logger, healthController)
		}),
		health.Module(),
	)
}
