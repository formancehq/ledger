package api

import (
	"github.com/formancehq/go-libs/v2/health"
	"github.com/formancehq/go-libs/v2/httpserver"
	"github.com/formancehq/ledger/internal/replication/controller"
	"go.uber.org/fx"
)

func NewModule(bind string) fx.Option {
	return fx.Options(
		fx.Provide(NewAPI),
		fx.Provide(func(ctrl *controller.Controller) Backend {
			return ctrl
		}),
		fx.Invoke(func(api *API, lc fx.Lifecycle) {
			lc.Append(httpserver.NewHook(api.Router(), httpserver.WithAddress(bind)))
		}),
		health.Module(),
	)
}
