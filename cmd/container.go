package cmd

import (
	"context"
	"github.com/numary/ledger/api"
	"github.com/numary/ledger/api/controllers"
	"github.com/numary/ledger/ledger"
	"github.com/numary/ledger/storage"
	"github.com/pkg/errors"
	"go.uber.org/fx"
)

type containerConfig struct {
	version        string
	storageDriver  string
	ledgerLister   controllers.LedgerLister
	basicAuth      string
	options        []fx.Option
	cache          bool
	rememberConfig bool
}

type option func(*containerConfig)

func WithVersion(version string) option {
	return func(c *containerConfig) {
		c.version = version
	}
}

func WithStorageDriver(driver string) option {
	return func(c *containerConfig) {
		c.storageDriver = driver
	}
}

func WithLedgerLister(lister controllers.LedgerLister) option {
	return func(c *containerConfig) {
		c.ledgerLister = lister
	}
}

func WithHttpBasicAuth(v string) option {
	return func(c *containerConfig) {
		c.basicAuth = v
	}
}

func WithOption(providers ...fx.Option) option {
	return func(c *containerConfig) {
		c.options = append(c.options, providers...)
	}
}

func WithCacheStorage(cache bool) option {
	return func(c *containerConfig) {
		c.cache = cache
	}
}

func WithRememberConfig(rememberConfig bool) option {
	return func(c *containerConfig) {
		c.rememberConfig = rememberConfig
	}
}

var DefaultOptions = []option{
	WithVersion("latest"),
	WithStorageDriver("sqlite"),
	WithLedgerLister(controllers.LedgerListerFn(func() []string {
		return []string{}
	})),
}

func NewContainer(options ...option) *fx.App {

	cfg := &containerConfig{}
	for _, opt := range append(DefaultOptions, options...) {
		opt(cfg)
	}

	providers := make([]interface{}, 0)
	providers = append(providers,
		fx.Annotate(func() string { return cfg.version }, fx.ResultTags(`name:"version"`)),
		fx.Annotate(func() string { return cfg.storageDriver }, fx.ResultTags(`name:"storageDriver"`)),
		fx.Annotate(func() controllers.LedgerLister { return cfg.ledgerLister }, fx.ResultTags(`name:"ledgerLister"`)),
		fx.Annotate(func() string { return cfg.basicAuth }, fx.ResultTags(`name:"httpBasic"`)),
		fx.Annotate(ledger.NewResolver, fx.ParamTags(`group:"resolverOptions"`)),
		fx.Annotate(
			ledger.WithStorageFactory,
			fx.ResultTags(`group:"resolverOptions"`),
			fx.As(new(ledger.ResolverOption)),
		),
		api.NewAPI,
		func() (f storage.Factory, err error) {
			f, err = storage.NewDefaultFactory(cfg.storageDriver)
			if err != nil {
				return nil, err
			}
			if cfg.cache {
				f = storage.NewCachedStorageFactory(f)
			}
			if cfg.rememberConfig {
				f = storage.NewRememberConfigStorageFactory(f)
			}
			return f, nil
		},
	)

	fxOptions := append(
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
		cfg.options...,
	)

	return fx.New(fxOptions...)
}
