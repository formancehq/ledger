module github.com/numary/ledger

go 1.16

require (
	github.com/XSAM/otelsql v0.10.0
	github.com/gin-contrib/cors v1.3.1
	github.com/gin-contrib/logger v0.2.0
	github.com/gin-gonic/gin v1.7.7
	github.com/google/go-cmp v0.5.6
	github.com/huandu/go-sqlbuilder v1.13.0
	github.com/jackc/pgconn v1.10.1
	github.com/jackc/pgx/v4 v4.14.1
	github.com/mattn/go-sqlite3 v1.14.9
	github.com/numary/machine v1.0.0
	github.com/ory/dockertest/v3 v3.8.1
	github.com/pborman/uuid v1.2.1
	github.com/pkg/errors v0.9.1
	github.com/sirupsen/logrus v1.8.1
	github.com/spf13/cobra v1.3.0
	github.com/spf13/viper v1.10.1
	github.com/stretchr/testify v1.7.0
	github.com/swaggo/swag v1.7.8
	go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin v0.28.0
	go.opentelemetry.io/otel v1.3.0
	go.opentelemetry.io/otel/exporters/jaeger v1.3.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric v0.26.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc v0.26.0
	go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp v0.26.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace v1.3.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.3.0
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp v1.3.0
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.3.0
	go.opentelemetry.io/otel/metric v0.26.0
	go.opentelemetry.io/otel/sdk v1.3.0
	go.opentelemetry.io/otel/sdk/metric v0.26.0
	go.opentelemetry.io/otel/trace v1.3.0
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/dig v1.13.0
	go.uber.org/fx v1.16.0
)

require (
	github.com/davecgh/go-spew v1.1.1
	github.com/go-logr/stdr v1.2.2 // indirect
)
