package cmd

import (
	"crypto/tls"
	"net/http"

	"github.com/formancehq/ledger/cmd/internal"
	"github.com/formancehq/ledger/pkg/api"
	"github.com/formancehq/ledger/pkg/api/middlewares"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/bus"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/redis"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/otlp/otlptraces"
	"github.com/formancehq/stack/libs/go-libs/publish"
	"github.com/formancehq/stack/libs/go-libs/service"
	"github.com/go-chi/cors"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const ServiceName = "ledger"

func resolveOptions(v *viper.Viper, userOptions ...fx.Option) []fx.Option {

	options := make([]fx.Option, 0)

	debug := v.GetBool(service.DebugFlag)
	if debug {
		sqlstorage.InstrumentalizeSQLDrivers()
	}

	options = append(options, publish.CLIPublisherModule(v, ServiceName), bus.LedgerMonitorModule())

	// Handle OpenTelemetry
	options = append(options, otlptraces.CLITracesModule(v))

	switch v.GetString(lockStrategyFlag) {
	case "redis":
		var tlsConfig *tls.Config
		if v.GetBool(lockStrategyRedisTLSEnabledFlag) {
			tlsConfig = &tls.Config{}
			if v.GetBool(lockStrategyRedisTLSInsecureFlag) {
				tlsConfig.InsecureSkipVerify = true
			}
		}
		options = append(options, redis.Module(redis.Config{
			Url:          v.GetString(lockStrategyRedisUrlFlag),
			LockDuration: v.GetDuration(lockStrategyRedisDurationFlag),
			LockRetry:    v.GetDuration(lockStrategyRedisRetryFlag),
			TLSConfig:    tlsConfig,
		}))
	}

	// Handle api part
	options = append(options, api.Module(api.Config{
		StorageDriver: v.GetString(storageDriverFlag),
		Version:       Version,
	}))

	// Handle storage driver
	options = append(options, sqlstorage.DriverModule(sqlstorage.ModuleConfig{
		StorageDriver: v.GetString(storageDriverFlag),
		SQLiteConfig: func() *sqlstorage.SQLiteConfig {
			if v.GetString(storageDriverFlag) != sqlstorage.SQLite.String() {
				return nil
			}
			return &sqlstorage.SQLiteConfig{
				Dir:    v.GetString(storageDirFlag),
				DBName: v.GetString(storageSQLiteDBNameFlag),
			}
		}(),
		PostgresConfig: func() *sqlstorage.PostgresConfig {
			if v.GetString(storageDriverFlag) != sqlstorage.PostgreSQL.String() {
				return nil
			}
			return &sqlstorage.PostgresConfig{
				ConnString: v.GetString(storagePostgresConnectionStringFlag),
			}
		}(),
	}))

	options = append(options, internal.NewAnalyticsModule(v, Version))

	options = append(options, fx.Provide(
		fx.Annotate(func() []ledger.LedgerOption {
			ledgerOptions := make([]ledger.LedgerOption, 0)

			if v.GetString(commitPolicyFlag) == "allow-past-timestamps" {
				ledgerOptions = append(ledgerOptions, ledger.WithPastTimestamps)
			}

			return ledgerOptions
		}, fx.ResultTags(ledger.ResolverLedgerOptionsKey)),
	))

	// Handle resolver
	options = append(options, ledger.ResolveModule(
		v.GetInt64(cacheCapacityBytes), v.GetInt64(cacheMaxNumKeys)))

	options = append(options, routes.ProvideMiddlewares(func(logger logging.Logger) []func(handler http.Handler) http.Handler {
		res := make([]func(handler http.Handler) http.Handler, 0)
		res = append(res, cors.New(cors.Options{
			AllowOriginFunc: func(r *http.Request, origin string) bool {
				return true
			},
			AllowCredentials: true,
		}).Handler)
		res = append(res, func(handler http.Handler) http.Handler {
			return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				handler.ServeHTTP(w, r.WithContext(
					logging.ContextWithLogger(r.Context(), logger),
				))
			})
		})
		res = append(res, middlewares.Log())
		return res
	}))

	return append(options, userOptions...)
}

func NewContainer(v *viper.Viper, userOptions ...fx.Option) *fx.App {
	return fx.New(resolveOptions(v, userOptions...)...)
}
