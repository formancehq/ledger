package cmd

import (
	"crypto/tls"
	"fmt"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedlogging/sharedlogginglogrus"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/controllers"
	"github.com/numary/ledger/pkg/api/middlewares"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/numary/ledger/pkg/bus"
	"github.com/numary/ledger/pkg/bus/httpbus"
	"github.com/numary/ledger/pkg/bus/kafkabus"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/opentelemetry/opentelemetrymetrics"
	"github.com/numary/ledger/pkg/opentelemetry/opentelemetrytraces"
	"github.com/numary/ledger/pkg/redis"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/dig"
	"go.uber.org/fx"
	"io"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
)

const ServiceName = "ledger"

func NewContainer(v *viper.Viper, options ...fx.Option) *fx.App {

	if !v.GetBool(debugFlag) {
		options = append(options, fx.NopLogger)
	}

	l := logrus.New()
	loggerFactory := sharedlogging.StaticLoggerFactory(sharedlogginglogrus.New(l))
	sharedlogging.SetFactory(loggerFactory)

	topics := v.GetStringSlice(publisherTopicMappingFlag)
	mapping := make(map[string]string)
	for _, topic := range topics {
		parts := strings.SplitN(topic, ":", 2)
		if len(parts) != 2 {
			panic("invalid topic flag")
		}
		mapping[parts[0]] = parts[1]
	}

	options = append(options, bus.Module())
	options = append(options, bus.ProvideMonitorOption(func() bus.MonitorOption {
		return bus.WithLedgerMonitorTopics(mapping)
	}))
	switch {
	case v.GetBool(publisherHttpEnabledFlag):
		options = append(options, httpbus.Module())
	case v.GetBool(publisherKafkaEnabledFlag):
		options = append(options, kafkabus.Module(ServiceName, v.GetStringSlice(publisherBusKafkaBrokerFlag)...))
	}

	// Handle OpenTelemetry
	if v.GetBool(otelTracesFlag) {
		options = append(options, opentelemetrytraces.TracesModule(opentelemetrytraces.ModuleConfig{
			ServiceName: ServiceName,
			Version:     Version,
			Batch:       v.GetBool(otelTracesBatchFlag),
			Exporter:    v.GetString(otelTracesExporterFlag),
			JaegerConfig: func() *opentelemetrytraces.JaegerConfig {
				if v.GetString(otelTracesExporterFlag) != opentelemetrytraces.JaegerExporter {
					return nil
				}
				return &opentelemetrytraces.JaegerConfig{
					Endpoint: v.GetString(otelTracesExporterJaegerEndpointFlag),
					User:     v.GetString(otelTracesExporterJaegerUserFlag),
					Password: v.GetString(otelTracesExporterJaegerPasswordFlag),
				}
			}(),
			OTLPConfig: func() *opentelemetrytraces.OTLPConfig {
				if v.GetString(otelTracesExporterFlag) != opentelemetrytraces.OTLPExporter {
					return nil
				}
				return &opentelemetrytraces.OTLPConfig{
					Mode:     v.GetString(otelTracesExporterOTLPModeFlag),
					Endpoint: v.GetString(otelTracesExporterOTLPEndpointFlag),
					Insecure: v.GetBool(otelTracesExporterOTLPInsecureFlag),
				}
			}(),
		}))
	}
	if v.GetBool(otelMetricsFlag) {
		options = append(options, opentelemetrymetrics.MetricsModule(opentelemetrymetrics.MetricsModuleConfig{
			Exporter: v.GetString(otelMetricsExporterFlag),
			OTLPConfig: func() *opentelemetrymetrics.OTLPMetricsConfig {
				if v.GetString(otelMetricsExporterFlag) != opentelemetrymetrics.OTLPMetricsExporter {
					return nil
				}
				return &opentelemetrymetrics.OTLPMetricsConfig{
					Mode:     v.GetString(otelMetricsExporterOTLPModeFlag),
					Endpoint: v.GetString(otelMetricsExporterOTLPEndpointFlag),
					Insecure: v.GetBool(otelMetricsExporterOTLPInsecureFlag),
				}
			}(),
		}))
	}

	switch v.GetString(lockStrategyFlag) {
	case "memory":
		options = append(options, ledger.MemoryLockModule())
	case "none":
		options = append(options, ledger.NoLockModule())
	case "redis":
		var tlsConfig *tls.Config
		if viper.GetBool(lockStrategyRedisTLSEnabledFlag) {
			tlsConfig = &tls.Config{}
			if viper.GetBool(lockStrategyRedisTLSInsecureFlag) {
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
		LedgerLister: controllers.LedgerListerFn(func(*http.Request) []string {
			return v.GetStringSlice(ledgersFlag)
		}),
		Version: Version,
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

	// Handle resolver
	options = append(options,
		ledger.ResolveModule(),
	)

	// fx has issues about decorators feature. We will wait until released.
	options = append(options,
		fx.Provide(func(params struct {
			dig.In
			Driver        storage.Driver
			MeterProvider metric.MeterProvider `optional:"true"`
		}) storage.Factory {
			f := storage.NewDefaultFactory(params.Driver)
			if v.GetBool(storageCacheFlag) {
				f = storage.NewCachedStorageFactory(f)
			}
			if v.GetBool(persistConfigFlag) {
				f = storage.NewRememberConfigStorageFactory(f)
			}
			if v.GetBool(otelTracesFlag) {
				f = opentelemetrytraces.WrapStorageFactory(f)
			}
			if v.GetBool(otelMetricsFlag) {
				f = opentelemetrymetrics.WrapStorageFactory(f, params.MeterProvider)
			}
			return f
		}),
	)

	// Api middlewares
	options = append(options, routes.ProvideMiddlewares(func(tp trace.TracerProvider) []gin.HandlerFunc {
		res := make([]gin.HandlerFunc, 0)

		cc := cors.DefaultConfig()
		cc.AllowAllOrigins = true
		cc.AllowCredentials = true
		cc.AddAllowHeaders("authorization")

		res = append(res, cors.New(cc))
		if viper.GetBool(otelTracesFlag) {
			res = append(res, otelgin.Middleware(ServiceName, otelgin.WithTracerProvider(tp)))
		} else {
			res = append(res, func(context *gin.Context) {
				context.Next()
				for _, err := range context.Errors {
					sharedlogging.GetLogger(context.Request.Context()).Error(err)
				}
			})
		}
		res = append(res, middlewares.Log())
		var writer io.Writer = os.Stderr
		if viper.GetBool(otelTracesFlag) {
			writer = ioutil.Discard
			res = append(res, opentelemetrytraces.Middleware())
		}
		res = append(res, gin.CustomRecoveryWithWriter(writer, func(c *gin.Context, err interface{}) {
			switch eerr := err.(type) {
			case error:
				c.AbortWithError(http.StatusInternalServerError, eerr)
			default:
				c.AbortWithError(http.StatusInternalServerError, fmt.Errorf("%s", err))
			}
		}))
		res = append(res, middlewares.Auth(viper.GetString(serverHttpBasicAuthFlag)))
		return res
	}, fx.ParamTags(`optional:"true"`)))

	return fx.New(options...)
}
