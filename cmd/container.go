package cmd

import (
	"context"
	"fmt"
	"github.com/XSAM/otelsql"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pkg/errors"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
	"net/http"
	"path"
)

func NewContainer(options ...fx.Option) *fx.App {

	providers := make([]interface{}, 0)
	providers = append(providers,
		func() (storage.Driver, error) {
			var (
				flavor             = sqlstorage.FlavorFromString(viper.GetString(storageDriverFlag))
				cached             bool
				connString         string
				connStringResolver sqlstorage.ConnStringResolver
			)
			switch flavor {
			case sqlstorage.PostgreSQL:
				cached = true
				connString = viper.GetString(storagePostgresConnectionStringFlagd)
			case sqlstorage.SQLite:
				connStringResolver = func(name string) string {
					return sqlstorage.SQLiteFileConnString(path.Join(
						viper.GetString(storageDirFlag),
						fmt.Sprintf("%s_%s.db", viper.GetString(storageSQLiteDBNameFlag), name),
					))
				}
			default:
				return nil, fmt.Errorf("Unknown storage driver: %s", viper.GetString(storageDirFlag))
			}

			if viper.GetBool(otelFlag) {
				sqlDriverName, err := otelsql.Register(
					sqlstorage.SQLDriverName(flavor),
					flavor.AttributeKeyValue().Value.AsString(),
				)
				if err != nil {
					return nil, fmt.Errorf("Error registering otel driver: %s", err)
				}
				sqlstorage.UpdateSQLDriverMapping(flavor, sqlDriverName)
			}

			var driver storage.Driver
			if cached {
				driver = sqlstorage.NewCachedDBDriver(flavor.String(), flavor, connString)
			} else {
				driver = sqlstorage.NewOpenCloseDBDriver(flavor.String(), flavor, connStringResolver)
			}

			return driver, nil
		},
		fx.Annotate(func() string { return "ledger" }, fx.ResultTags(`name:"serviceName"`)),
		fx.Annotate(func() string { return Version }, fx.ResultTags(`name:"version"`)),
		fx.Annotate(func(driver storage.Driver) string { return driver.Name() }, fx.ResultTags(`name:"storageDriver"`)),
		fx.Annotate(func() controllers.LedgerLister {
			return controllers.LedgerListerFn(func(*http.Request) []string {
				return viper.GetStringSlice(ledgersFlag)
			})
		}, fx.ResultTags(`name:"ledgerLister"`)),
		fx.Annotate(func() string { return viper.GetString(serverHttpBasicAuthFlag) }, fx.ResultTags(`name:"httpBasic"`)),
		fx.Annotate(ledger.NewResolver, fx.ParamTags(`group:"resolverOptions"`)),
		fx.Annotate(
			ledger.WithStorageFactory,
			fx.ResultTags(`group:"resolverOptions"`),
			fx.As(new(ledger.ResolverOption)),
		),
		api.NewAPI,
		func(driver storage.Driver) storage.Factory {
			f := storage.NewDefaultFactory(driver)
			if viper.GetBool(storageCacheFlag) {
				f = storage.NewCachedStorageFactory(f)
			}
			if viper.GetBool(persistConfigFlag) {
				f = storage.NewRememberConfigStorageFactory(f)
			}
			if viper.GetBool(otelFlag) {
				f = opentelemetry.NewOpenTelemetryStorageFactory(f)
			}
			return f
		},
	)
	invokes := make([]interface{}, 0)
	if viper.GetBool(otelFlag) {
		switch viper.GetString(otelExporterFlag) {
		case "stdout":
			options = append(options, opentelemetry.StdoutModule())
		case "jaeger":
			options = append(options, opentelemetry.JaegerModule())
		case "noop":
			options = append(options, opentelemetry.NoOpModule())
		}
		options = append(options, routes.ProvideGlobalMiddleware(func(tracerProvider trace.TracerProvider) gin.HandlerFunc {
			return otelgin.Middleware("ledger", otelgin.WithTracerProvider(tracerProvider))
		}))
	}
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

	fxOptions := []fx.Option{
		fx.Provide(providers...),
		fx.Invoke(invokes...),
		api.Module,
	}

	return fx.New(
		append(fxOptions, options...)...,
	)
}
