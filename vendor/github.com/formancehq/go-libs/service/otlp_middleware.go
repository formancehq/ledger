package service

import (
	"net/http"
	"net/http/httputil"

	"github.com/riandyrn/otelchi"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type responseWriter struct {
	http.ResponseWriter
	data []byte
}

func (w *responseWriter) Write(data []byte) (int, error) {
	if w.Header().Get("Content-Type") == "application/octet-stream" {
		return w.ResponseWriter.Write(data)
	}
	w.data = append(w.data, data...)
	return len(data), nil
}

func (w *responseWriter) finalize() {
	if w.Header().Get("Content-Type") == "application/octet-stream" {
		return
	}
	_, err := w.ResponseWriter.Write(w.data)
	if err != nil {
		panic(err)
	}
}

func OTLPMiddleware(serverName string, debug bool) func(h http.Handler) http.Handler {
	m := otelchi.Middleware(serverName)
	return func(h http.Handler) http.Handler {
		return m(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if debug {
				data, err := httputil.DumpRequest(r, true)
				if err != nil {
					panic(err)
				}
				trace.SpanFromContext(r.Context()).
					SetAttributes(attribute.String("http.request", string(data)))

				rw := &responseWriter{w, make([]byte, 0, 1024)}
				defer func() {
					rw.finalize()

					trace.SpanFromContext(r.Context()).
						SetAttributes(attribute.String("http.response", string(rw.data)))
				}()

				h.ServeHTTP(rw, r)
				return
			}

			h.ServeHTTP(w, r)
		}))
	}
}
