package cmd

import (
	"crypto/tls"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
	"github.com/numary/go-libs/oauth2/oauth2introspect"
	"github.com/numary/go-libs/sharedauth"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedlogging/sharedlogginglogrus"
	"github.com/numary/go-libs/sharedotlp/pkg/sharedotlpmetrics"
	"github.com/numary/go-libs/sharedotlp/pkg/sharedotlptraces"
	"github.com/numary/go-libs/sharedpublish"
	"github.com/numary/go-libs/sharedpublish/sharedpublishhttp"
	"github.com/numary/go-libs/sharedpublish/sharedpublishkafka"
	"github.com/numary/ledger/cmd/internal"
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
	"github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/xdg-go/scram"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

const (
	ServiceName = "ledger"

	TraceIdHeader = "trace-id"
)

func NewContainer(v *viper.Viper, stdout io.Writer, userOptions ...fx.Option) *fx.App {

	options := make([]fx.Option, 0)
	if !v.GetBool(DebugFlag) {
		options = append(options, fx.NopLogger)
	}

	l := logrus.New()
	l.SetOutput(stdout)
	if v.GetBool(DebugFlag) {
		l.Level = logrus.DebugLevel
	}
	loggerFactory := sharedlogging.StaticLoggerFactory(sharedlogginglogrus.New(l))
	sharedlogging.SetFactory(loggerFactory)

	topics := v.GetStringSlice(PublisherTopicMappingFlag)
	mapping := make(map[string]string)
	for _, topic := range topics {
		parts := strings.SplitN(topic, ":", 2)
		if len(parts) != 2 {
			panic("invalid topic flag")
		}
		mapping[parts[0]] = parts[1]
	}

	options = append(options, sharedpublish.Module(), bus.LedgerMonitorModule())
	options = append(options, sharedpublish.TopicMapperPublisherModule(mapping))

	switch {
	case v.GetBool(PublisherHttpEnabledFlag):
		options = append(options, sharedpublishhttp.Module())
	case v.GetBool(PublisherKafkaEnabledFlag):
		sarama.Logger = log.New(os.Stdout, "[Sarama] ", log.LstdFlags)
		options = append(options,
			sharedpublishkafka.Module(ServiceName, v.GetStringSlice(PublisherKafkaBrokerFlag)...),
			sharedpublishkafka.ProvideSaramaOption(
				sharedpublishkafka.WithConsumerReturnErrors(),
				sharedpublishkafka.WithProducerReturnSuccess(),
			),
		)
		if v.GetBool(PublisherKafkaTLSEnabled) {
			options = append(options, sharedpublishkafka.ProvideSaramaOption(sharedpublishkafka.WithTLS()))
		}
		if v.GetBool(PublisherKafkaSASLEnabled) {
			options = append(options, sharedpublishkafka.ProvideSaramaOption(
				sharedpublishkafka.WithSASLEnabled(),
				sharedpublishkafka.WithSASLCredentials(
					v.GetString(PublisherKafkaSASLUsername),
					v.GetString(PublisherKafkaSASLPassword),
				),
				sharedpublishkafka.WithSASLMechanism(sarama.SASLMechanism(v.GetString(PublisherKafkaSASLMechanism))),
				sharedpublishkafka.WithSASLScramClient(func() sarama.SCRAMClient {
					var fn scram.HashGeneratorFcn
					switch v.GetInt(PublisherKafkaSASLScramSHASize) {
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
	if m := sharedotlptraces.CLITracesModule(v); m != nil {
		options = append(options, m)
	}
	if m := sharedotlpmetrics.CLIMetricsModule(v); m != nil {
		options = append(options, m)
	}

	switch v.GetString(LockStrategyFlag) {
	case "memory":
		options = append(options, ledger.MemoryLockModule())
	case "none":
		options = append(options, ledger.NoLockModule())
	case "redis":
		var tlsConfig *tls.Config
		if v.GetBool(LockStrategyRedisTLSEnabledFlag) {
			tlsConfig = &tls.Config{}
			if v.GetBool(LockStrategyRedisTLSInsecureFlag) {
				tlsConfig.InsecureSkipVerify = true
			}
		}
		options = append(options, redis.Module(redis.Config{
			Url:          v.GetString(LockStrategyRedisUrlFlag),
			LockDuration: v.GetDuration(LockStrategyRedisDurationFlag),
			LockRetry:    v.GetDuration(LockStrategyRedisRetryFlag),
			TLSConfig:    tlsConfig,
		}))
	}

	// Handle api part
	options = append(options, api.Module(api.Config{
		StorageDriver: v.GetString(StorageDriverFlag),
		Version:       Version,
		UseScopes:     viper.GetBool(AuthBearerUseScopesFlag),
	}))

	// Handle storage driver
	options = append(options, sqlstorage.DriverModule(sqlstorage.ModuleConfig{
		StorageDriver: v.GetString(StorageDriverFlag),
		SQLiteConfig: func() *sqlstorage.SQLiteConfig {
			if v.GetString(StorageDriverFlag) != sqlstorage.SQLite.String() {
				return nil
			}
			return &sqlstorage.SQLiteConfig{
				Dir:    v.GetString(StorageDirFlag),
				DBName: v.GetString(StorageSQLiteDBNameFlag),
			}
		}(),
		PostgresConfig: func() *sqlstorage.PostgresConfig {
			if v.GetString(StorageDriverFlag) != sqlstorage.PostgreSQL.String() {
				return nil
			}
			return &sqlstorage.PostgresConfig{
				ConnString: v.GetString(StoragePostgresConnectionStringFlag),
			}
		}(),
	}))

	options = append(options, internal.NewAnalyticsModule(v, Version))

	options = append(options, fx.Provide(
		fx.Annotate(func() []ledger.LedgerOption {
			ledgerOptions := []ledger.LedgerOption{}

			if v.GetString(CommitPolicyFlag) == "allow-past-timestamps" {
				ledgerOptions = append(ledgerOptions, ledger.WithPastTimestamps)
			}

			return ledgerOptions
		}, fx.ResultTags(ledger.ResolverLedgerOptionsKey)),
	))

	// Handle resolver
	options = append(options,
		ledger.ResolveModule(),
	)

	options = append(options,
		fx.Decorate(fx.Annotate(func(driver storage.Driver[ledger.Store], mp metric.MeterProvider) storage.Driver[ledger.Store] {
			if v.GetBool(sharedotlptraces.OtelTracesFlag) {
				driver = opentelemetrytraces.WrapStorageDriver(driver)
			}
			if v.GetBool(sharedotlpmetrics.OtelMetricsFlag) {
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
		if v.GetBool(AuthBearerEnabledFlag) {
			methods = append(methods, sharedauth.NewHttpBearerMethod(
				sharedauth.NewIntrospectionValidator(
					oauth2introspect.NewIntrospecter(v.GetString(AuthBearerIntrospectUrlFlag)),
					v.GetBool(AuthBearerAudiencesWildcardFlag),
					sharedauth.AudienceIn(v.GetStringSlice(AuthBearerAudienceFlag)...),
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
		if v.GetBool(sharedotlptraces.OtelTracesFlag) {
			res = append(res, otelgin.Middleware(ServiceName, otelgin.WithTracerProvider(tp)))
			res = append(res, func(c *gin.Context) {
				span := trace.SpanFromContext(c.Request.Context())
				c.Writer.Header().Set(TraceIdHeader, span.SpanContext().TraceID().String())
			})
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
		if v.GetBool(sharedotlptraces.OtelTracesFlag) {
			writer = io.Discard
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
