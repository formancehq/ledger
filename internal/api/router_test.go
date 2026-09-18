package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ThreeDotsLabs/watermill/message"
	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/trace"
	nooptracer "go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v5/pkg/authn/jwt"
	"github.com/formancehq/ledger/internal/controller/system"
)

type noopPublisher struct{}

func (noopPublisher) Publish(string, ...*message.Message) error { return nil }
func (noopPublisher) Close() error                              { return nil }

type noopSystemController struct{ system.Controller }

func TestModulePassesMeterProviderToRouter(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	meterProvider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	var router chi.Router

	app := fx.New(
		Module(Config{Version: "test"}),
		fx.Provide(
			func() system.Controller { return noopSystemController{} },
			func() jwt.Authenticator { return jwt.NewNoAuth() },
			func() message.Publisher { return noopPublisher{} },
			func() metric.MeterProvider { return meterProvider },
			func() trace.TracerProvider { return nooptracer.NewTracerProvider() },
		),
		fx.Populate(&router),
	)
	require.NoError(t, app.Err())
	require.NoError(t, app.Start(context.Background()))

	router.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/not-found", nil))

	var resourceMetrics metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &resourceMetrics))
	require.NoError(t, app.Stop(context.Background()))

	names := make(map[string]struct{})
	for _, scopeMetrics := range resourceMetrics.ScopeMetrics {
		for _, metric := range scopeMetrics.Metrics {
			names[metric.Name] = struct{}{}
		}
	}

	for _, name := range []string{
		"request_duration_millis",
		"requests_inflight",
		"response_size_bytes",
	} {
		_, ok := names[name]
		require.Truef(t, ok, "expected API metric %q to be exported", name)
	}
}
