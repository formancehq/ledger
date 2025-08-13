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
	debug      bool
}

func (t WithBodiesTracingHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	rawRequest, err := httputil.DumpRequest(req, true)
	if err != nil {
		panic(err)
	}

	rsp, err := t.underlying.RoundTrip(req)
	if t.debug || err != nil || rsp.StatusCode >= 400 {
		span := trace.SpanFromContext(req.Context())
		span.SetAttributes(attribute.String("raw-request", string(rawRequest)))
		if err != nil {
			span.SetAttributes(attribute.String("http-error", err.Error()))
		}
		if rsp != nil {
			rawResponse, err := httputil.DumpResponse(rsp, true)
			if err != nil {
				panic(err)
			}

			span.SetAttributes(attribute.String("raw-response", string(rawResponse)))
		}
	}

	return rsp, err
}

func NewRoundTripper(httpTransport http.RoundTripper, debug bool, options ...otelhttp.Option) http.RoundTripper {
	var transport = httpTransport
	transport = WithBodiesTracingHTTPTransport{
		underlying: transport,
		debug:      debug,
	}
	return otelhttp.NewTransport(transport, options...)
}
