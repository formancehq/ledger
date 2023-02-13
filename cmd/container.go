package cmd

import (
	"crypto/tls"
	"fmt"
	"net/http"
	"os"

	"github.com/formancehq/stack/libs/go-libs/auth"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/logging/logginglogrus"
	"github.com/formancehq/stack/libs/go-libs/oauth2/oauth2introspect"
	"github.com/formancehq/stack/libs/go-libs/otlp/otlptraces"
	"github.com/formancehq/stack/libs/go-libs/publish"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/ledger/cmd/internal"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/middlewares"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/numary/ledger/pkg/bus"
	"github.com/numary/ledger/pkg/contextlogger"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/redis"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/uptrace/opentelemetry-go-extra/otellogrus"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

const ServiceName = "ledger"

func NewContainer(v *viper.Viper, userOptions ...fx.Option) *fx.App {

	options := make([]fx.Option, 0)
	if !v.GetBool(debugFlag) {
		options = append(options, fx.NopLogger)
	}

	debug := viper.GetBool(debugFlag)

	l := logrus.New()
	if debug {
		l.Level = logrus.DebugLevel
	}
	if viper.GetBool(otlptraces.OtelTracesFlag) {
		l.AddHook(otellogrus.NewHook(otellogrus.WithLevels(
			logrus.PanicLevel,
			logrus.FatalLevel,
			logrus.ErrorLevel,
			logrus.WarnLevel,
		)))
	}
	logging.SetFactory(contextlogger.NewFactory(
		logging.StaticLoggerFactory(logginglogrus.New(l)),
	))
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
		UseScopes:     viper.GetBool(authBearerUseScopesFlag),
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
			ledgerOptions := []ledger.LedgerOption{}

			if v.GetString(commitPolicyFlag) == "allow-past-timestamps" {
				ledgerOptions = append(ledgerOptions, ledger.WithPastTimestamps)
			}

			return ledgerOptions
		}, fx.ResultTags(ledger.ResolverLedgerOptionsKey)),
	))

	// Handle resolver
	options = append(options, ledger.ResolveModule(
		v.GetInt64(cacheCapacityBytes), v.GetInt64(cacheMaxNumKeys)))

	// Api middlewares
	options = append(options, routes.ProvidePerLedgerMiddleware(func(tp trace.TracerProvider) []gin.HandlerFunc {
		res := make([]gin.HandlerFunc, 0)

		methods := make([]auth.Method, 0)
		if httpBasicMethod := internal.HTTPBasicAuthMethod(v); httpBasicMethod != nil {
			methods = append(methods, httpBasicMethod)
		}
		if v.GetBool(authBearerEnabledFlag) {
			methods = append(methods, auth.NewHttpBearerMethod(
				auth.NewIntrospectionValidator(
					oauth2introspect.NewIntrospecter(v.GetString(authBearerIntrospectUrlFlag)),
					v.GetBool(authBearerAudiencesWildcardFlag),
					auth.AudienceIn(v.GetStringSlice(authBearerAudienceFlag)...),
				),
			))
		}
		if len(methods) > 0 {
			res = append(res, func(c *gin.Context) {
				handled := false
				auth.Middleware(methods...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					handled = true
					// The middleware replace the context of the request to include the agent
					// We have to forward it to gin
					c.Request = r
					c.Next()
				})).ServeHTTP(c.Writer, c.Request)
				if !handled {
					c.Abort()
				}
			})
		}
		return res
	}, fx.ParamTags(`optional:"true"`)))

	options = append(options, routes.ProvideMiddlewares(func(tp trace.TracerProvider) []gin.HandlerFunc {
		res := make([]gin.HandlerFunc, 0)

		cc := cors.DefaultConfig()
		cc.AllowAllOrigins = true
		cc.AllowCredentials = true
		cc.AddAllowHeaders("authorization")

		res = append(res, cors.New(cc))
		res = append(res, func(context *gin.Context) {
			context.Next()
			for _, err := range context.Errors {
				logging.GetLogger(context.Request.Context()).Error(err)
			}
		})
		res = append(res, middlewares.Log())
		res = append(res, gin.CustomRecoveryWithWriter(os.Stderr, func(c *gin.Context, err interface{}) {
			switch eerr := err.(type) {
			case error:
				_ = c.AbortWithError(http.StatusInternalServerError, eerr)
			default:
				_ = c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("%s", err))
			}
		}))
		return res
	}, fx.ParamTags(`optional:"true"`)))

	return fx.New(append(options, userOptions...)...)
}
