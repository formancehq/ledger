package api

import (
	"context"
	_ "embed"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/api/controllers"
	"github.com/numary/ledger/ledger"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

// Module exported for initializing application
var Module = fx.Options(
	controllers.Module,
)

type API struct {
	addr   string
	engine *gin.Engine
}

func NewAPI(
	lc fx.Lifecycle,
	resolver *ledger.Resolver,
	configController *controllers.ConfigController, //todo: use fx
	ledgerController *controllers.LedgerController, //todo: use fx
	scriptController *controllers.ScriptController, //todo: use fx
	accountController *controllers.AccountController, //todo: use fx
	transactionController *controllers.TransactionController, //todo: use fx
) *API {
	gin.SetMode(gin.ReleaseMode)

	cc := cors.DefaultConfig()
	cc.AllowAllOrigins = true
	cc.AllowCredentials = true
	cc.AddAllowHeaders("authorization")

	//todo: use fx
	router := NewRoutes(
		cc,
		resolver,
		configController,
		ledgerController,
		scriptController,
		accountController,
		transactionController,
	)

	h := &API{
		engine: router, //todo: use fx
		addr:   viper.GetString("server.http.bind_address"),
	}

	lc.Append(fx.Hook{
		OnStart: func(c context.Context) error {
			go h.Start()
			return nil
		},
	})

	return h
}

func (h *API) Start() {
	h.engine.Run(h.addr)
}
