package otlp

import (
	"net/http"
	"net/http/httputil"

	"go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type WithBodiesTracingHTTPTransport struct {
	underlying http.RoundTripper
}

func (t WithBodiesTracingHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	span := trace.SpanFromContext(req.Context())
	rawRequest, err := httputil.DumpRequest(req, true)
	if err != nil {
		panic(err)
	}

	span.SetAttributes(attribute.String("raw-request", string(rawRequest)))

	rsp, err := t.underlying.RoundTrip(req)
	if rsp != nil {
		rawResponse, err := httputil.DumpResponse(rsp, true)
		if err != nil {
			panic(err)
		}

		span.SetAttributes(attribute.String("raw-response", string(rawResponse)))
	}
	return rsp, err
}

func NewRoundTripper(debug bool) http.RoundTripper {
	var transport = http.DefaultTransport
	if debug {
		transport = WithBodiesTracingHTTPTransport{transport}
	}
	return otelhttp.NewTransport(transport)
}
