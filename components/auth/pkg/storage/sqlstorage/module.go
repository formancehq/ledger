package sqlstorage

import (
	"crypto/rsa"

	auth "github.com/formancehq/auth/pkg"
	"github.com/formancehq/auth/pkg/oidc"
	"github.com/formancehq/go-libs/health"
	"github.com/zitadel/oidc/pkg/op"
	"go.uber.org/fx"
)

func Module(kind, uri string, debug bool, key *rsa.PrivateKey, staticClients ...auth.StaticClient) fx.Option {
	return fx.Options(
		gormModule(kind, uri, debug),
		fx.Supply(key),
		fx.Supply(staticClients),
		fx.Provide(fx.Annotate(New,
			fx.As(new(oidc.Storage)),
		)),
		health.ProvideHealthCheck(func(storage op.Storage) health.NamedCheck {
			return health.NewNamedCheck("Database", health.CheckFn(storage.Health))
		}),
	)
}
