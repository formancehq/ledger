package auth

import (
	"github.com/formancehq/go-libs/logging"
	"go.uber.org/fx"
)

type ModuleConfig struct {
	Enabled              bool
	Issuer               string
	ReadKeySetMaxRetries int
	CheckScopes          bool
	Service              string
}

func Module(cfg ModuleConfig) fx.Option {
	options := make([]fx.Option, 0)

	options = append(options,
		fx.Provide(func() Authenticator {
			return NewNoAuth()
		}),
	)

	if cfg.Enabled {
		options = append(options,
			fx.Decorate(func(logger logging.Logger) Authenticator {
				return newJWTAuth(
					logger,
					cfg.ReadKeySetMaxRetries,
					cfg.Issuer,
					cfg.Service,
					cfg.CheckScopes,
				)
			}),
		)
	}

	return fx.Options(options...)
}
