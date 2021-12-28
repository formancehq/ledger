package cmd

import (
	"context"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pkg/errors"
	"go.uber.org/fx"
	"net/http"
)

type containerConfig struct {
	version        string
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
	WithLedgerLister(controllers.LedgerListerFn(func(*http.Request) []string {
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
		fx.Annotate(func() string { return "ledger" }, fx.ResultTags(`name:"serviceName"`)),
		fx.Annotate(func() string { return cfg.version }, fx.ResultTags(`name:"version"`)),
		fx.Annotate(func(driver storage.Driver) string { return driver.Name() }, fx.ResultTags(`name:"storageDriver"`)),
		fx.Annotate(func() controllers.LedgerLister { return cfg.ledgerLister }, fx.ResultTags(`name:"ledgerLister"`)),
		fx.Annotate(func() string { return cfg.basicAuth }, fx.ResultTags(`name:"httpBasic"`)),
		fx.Annotate(ledger.NewResolver, fx.ParamTags(`group:"resolverOptions"`)),
		fx.Annotate(
			ledger.WithStorageFactory,
			fx.ResultTags(`group:"resolverOptions"`),
			fx.As(new(ledger.ResolverOption)),
		),
		api.NewAPI,
		func(driver storage.Driver) storage.Factory {
			f := storage.NewDefaultFactory(driver)
			if cfg.cache {
				f = storage.NewCachedStorageFactory(f)
			}
			if cfg.rememberConfig {
				f = storage.NewRememberConfigStorageFactory(f)
			}
			f = opentelemetry.NewOpenTelemetryStorageFactory(f)
			return f
		},
	)
	invokes := make([]interface{}, 0)
	invokes = append(invokes, func(driver storage.Driver, lifecycle fx.Lifecycle) error {
		err := driver.Initialize(context.Background())
		if err != nil {
			return errors.Wrap(err, "initializing driver")
		}
		lifecycle.Append(fx.Hook{
			OnStop: driver.Close,
		})
		return nil
	})
	fxOptions := append(
		[]fx.Option{
			fx.Provide(providers...),
			fx.Invoke(invokes...),
			api.Module,
		},
		cfg.options...,
	)

	return fx.New(fxOptions...)
}
