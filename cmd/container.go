package cmd

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/Masterminds/semver/v3"
	"github.com/Shopify/sarama"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/oauth2/oauth2introspect"
	"github.com/numary/go-libs/sharedauth"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedlogging/sharedlogginglogrus"
	"github.com/numary/go-libs/sharedotlp/sharedotlpmetrics"
	"github.com/numary/go-libs/sharedotlp/sharedotlptraces"
	"github.com/numary/go-libs/sharedpublish"
	"github.com/numary/go-libs/sharedpublish/sharedpublishhttp"
	"github.com/numary/go-libs/sharedpublish/sharedpublishkafka"
	"github.com/numary/ledger/cmd/internal"
	"github.com/numary/ledger/pkg/analytics"
	"github.com/numary/ledger/pkg/api"
	"github.com/numary/ledger/pkg/api/middlewares"
	"github.com/numary/ledger/pkg/api/routes"
	"github.com/numary/ledger/pkg/bus"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/opentelemetry/opentelemetrymetrics"
	"github.com/numary/ledger/pkg/opentelemetry/opentelemetrytraces"
	"github.com/numary/ledger/pkg/redis"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/xdg-go/scram"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

const ServiceName = "ledger"

func NewContainer(v *viper.Viper, userOptions ...fx.Option) *fx.App {

	options := make([]fx.Option, 0)
	if !v.GetBool(debugFlag) {
		options = append(options, fx.NopLogger)
	}

	l := logrus.New()
	if v.GetBool(debugFlag) {
		l.Level = logrus.DebugLevel
	}
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

	if v.GetBool(segmentEnabledFlag) {
		applicationId := viper.GetString(segmentApplicationId)
		if applicationId == "" {
			applicationId = uuid.New()
		}
		writeKey := viper.GetString(segmentWriteKey)
		interval := viper.GetDuration(segmentHeartbeatInterval)
		if writeKey == "" {
			sharedlogging.GetLogger(context.Background()).Infof("Segment enabled but no write key provided")
		} else if interval == 0 {
			sharedlogging.GetLogger(context.Background()).Error("Segment heartbeat interval is 0")
		} else {
			_, err := semver.NewVersion(Version)
			if err != nil {
				sharedlogging.GetLogger(context.Background()).Infof("Segment enabled but version '%s' is not semver, skip", Version)
			} else {
				options = append(options, analytics.NewHeartbeatModule(applicationId, Version, writeKey, interval))
			}
		}
	}

	options = append(options, sharedpublish.Module(), bus.LedgerMonitorModule())
	options = append(options, sharedpublish.TopicMapperPublisherModule(mapping))

	switch {
	case v.GetBool(publisherHttpEnabledFlag):
		options = append(options, sharedpublishhttp.Module())
	case v.GetBool(publisherKafkaEnabledFlag):
		sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
		options = append(options,
			sharedpublishkafka.Module(ServiceName, v.GetStringSlice(publisherKafkaBrokerFlag)...),
			sharedpublishkafka.ProvideSaramaOption(
				sharedpublishkafka.WithConsumerReturnErrors(),
				sharedpublishkafka.WithProducerReturnSuccess(),
			),
		)
		if v.GetBool(publisherKafkaTLSEnabled) {
			options = append(options, sharedpublishkafka.ProvideSaramaOption(sharedpublishkafka.WithTLS()))
		}
		if v.GetBool(publisherKafkaSASLEnabled) {
			options = append(options, sharedpublishkafka.ProvideSaramaOption(
				sharedpublishkafka.WithSASLEnabled(),
				sharedpublishkafka.WithSASLCredentials(
					v.GetString(publisherKafkaSASLUsername),
					v.GetString(publisherKafkaSASLPassword),
				),
				sharedpublishkafka.WithSASLMechanism(sarama.SASLMechanism(v.GetString(publisherKafkaSASLMechanism))),
				sharedpublishkafka.WithSASLScramClient(func() sarama.SCRAMClient {
					var fn scram.HashGeneratorFcn
					switch v.GetInt(publisherKafkaSASLScramSHASize) {
					case 512:
						fn = sharedpublishkafka.SHA512
					case 256:
						fn = sharedpublishkafka.SHA256
					default:
						panic("sha size not handled")
					}
					return &sharedpublishkafka.XDGSCRAMClient{
						HashGeneratorFcn: fn,
					}
				}),
			))
		}
	}

	// Handle OpenTelemetry
	if v.GetBool(otelTracesFlag) {
		options = append(options, sharedotlptraces.TracesModule(sharedotlptraces.ModuleConfig{
			Batch:    v.GetBool(otelTracesBatchFlag),
			Exporter: v.GetString(otelTracesExporterFlag),
			JaegerConfig: func() *sharedotlptraces.JaegerConfig {
				if v.GetString(otelTracesExporterFlag) != sharedotlptraces.JaegerExporter {
					return nil
				}
				return &sharedotlptraces.JaegerConfig{
					Endpoint: v.GetString(otelTracesExporterJaegerEndpointFlag),
					User:     v.GetString(otelTracesExporterJaegerUserFlag),
					Password: v.GetString(otelTracesExporterJaegerPasswordFlag),
				}
			}(),
			OTLPConfig: func() *sharedotlptraces.OTLPConfig {
				if v.GetString(otelTracesExporterFlag) != sharedotlptraces.OTLPExporter {
					return nil
				}
				return &sharedotlptraces.OTLPConfig{
					Mode:     v.GetString(otelTracesExporterOTLPModeFlag),
					Endpoint: v.GetString(otelTracesExporterOTLPEndpointFlag),
					Insecure: v.GetBool(otelTracesExporterOTLPInsecureFlag),
				}
			}(),
		}))
	}
	if v.GetBool(otelMetricsFlag) {
		options = append(options, sharedotlpmetrics.MetricsModule(sharedotlpmetrics.MetricsModuleConfig{
			Exporter: v.GetString(otelMetricsExporterFlag),
			OTLPConfig: func() *sharedotlpmetrics.OTLPMetricsConfig {
				if v.GetString(otelMetricsExporterFlag) != sharedotlpmetrics.OTLPMetricsExporter {
					return nil
				}
				return &sharedotlpmetrics.OTLPMetricsConfig{
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

	// Handle resolver
	options = append(options,
		ledger.ResolveModule(),
	)

	options = append(options,
		fx.Decorate(fx.Annotate(func(driver storage.Driver, mp metric.MeterProvider) storage.Driver {
			if v.GetBool(storageCacheFlag) {
				driver = storage.NewCachedStorageDriver(driver)
			}
			if v.GetBool(otelTracesFlag) {
				driver = opentelemetrytraces.WrapStorageDriver(driver)
			}
			if v.GetBool(otelMetricsFlag) {
				driver = opentelemetrymetrics.WrapStorageDriver(driver, mp)
			}
			return driver
		}, fx.ParamTags(``, `optional:"true"`))),
	)

	// Api middlewares
	options = append(options, routes.ProvidePerLedgerMiddleware(func(tp trace.TracerProvider) []gin.HandlerFunc {
		res := make([]gin.HandlerFunc, 0)

		methods := make([]sharedauth.Method, 0)
		if httpBasicMethod := internal.HTTPBasicAuthMethod(v); httpBasicMethod != nil {
			methods = append(methods, httpBasicMethod)
		}
		if v.GetBool(authBearerEnabledFlag) {
			methods = append(methods, sharedauth.NewHttpBearerMethod(
				sharedauth.NewIntrospectionValidator(
					oauth2introspect.NewIntrospecter(v.GetString(authBearerIntrospectUrlFlag)),
					v.GetBool(authBearerAudiencesWildcardFlag),
					sharedauth.AudienceIn(v.GetStringSlice(authBearerAudienceFlag)...),
				),
			))
		}
		if len(methods) > 0 {
			res = append(res, func(c *gin.Context) {
				handled := false
				sharedauth.Middleware(methods...)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
		if v.GetBool(otelTracesFlag) {
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
		if v.GetBool(otelTracesFlag) {
			writer = ioutil.Discard
			res = append(res, opentelemetrytraces.Middleware())
		}
		res = append(res, gin.CustomRecoveryWithWriter(writer, func(c *gin.Context, err interface{}) {
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
