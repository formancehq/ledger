package otlp

import (
	"fmt"
	"net/http"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.uber.org/fx"
)

func HttpClientModule() fx.Option {
	return fx.Provide(func() *http.Client {
		return &http.Client{
			Transport: otelhttp.NewTransport(http.DefaultTransport, otelhttp.WithSpanNameFormatter(func(operation string, r *http.Request) string {
				str := fmt.Sprintf("%s %s", r.Method, r.URL.Path)
				if len(r.URL.Query()) == 0 {
					return str
				}

				return fmt.Sprintf("%s?%s", str, r.URL.Query().Encode())
			})),
		}
	})
}
