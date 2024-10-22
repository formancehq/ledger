package common

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/api"
	"net/http"
	"sync"
	"time"
)

type responseWriter struct {
	http.ResponseWriter
	sync.Mutex
	contextWithTimeout context.Context
	headerWritten      bool
}

func (r *responseWriter) Header() http.Header {
	r.Lock()
	defer r.Unlock()

	return r.ResponseWriter.Header()
}

func (r *responseWriter) Write(bytes []byte) (int, error) {
	r.Lock()
	defer r.Unlock()

	// As the http status code has already been sent, we ignore the context canceled in this case.
	// The timeout is usually shorter than the allowed window.
	// We let a chance to the client to get their data.
	if r.headerWritten {
		return r.ResponseWriter.Write(bytes)
	}

	select {
	case <-r.contextWithTimeout.Done():
		return 0, nil
	default:
		return r.ResponseWriter.Write(bytes)
	}
}

func (r *responseWriter) WriteHeader(statusCode int) {
	r.Lock()
	defer r.Unlock()

	select {
	case <-r.contextWithTimeout.Done():
	default:
		r.ResponseWriter.WriteHeader(statusCode)
		r.headerWritten = true
	}
}

var _ http.ResponseWriter = &responseWriter{}

type TimeoutConfiguration struct {
	Timeout    time.Duration
	StatusCode int
}

func Timeout(configuration TimeoutConfiguration) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			newRequestContext, cancelRequestContext := context.WithCancel(r.Context())
			defer cancelRequestContext()

			contextWithTimeout, cancelContextWithTimeout := context.WithTimeout(context.Background(), configuration.Timeout)
			defer cancelContextWithTimeout()

			rw := &responseWriter{
				ResponseWriter:     w,
				contextWithTimeout: contextWithTimeout,
			}
			done := make(chan struct{})
			go func() {
				select {
				case <-done:
				case <-contextWithTimeout.Done():
					rw.Lock()
					defer rw.Unlock()

					if !rw.headerWritten {
						cancelRequestContext()
						api.WriteErrorResponse(w, configuration.StatusCode, "TIMEOUT", fmt.Errorf("request timed out"))
					}
				}
			}()
			defer close(done)

			h.ServeHTTP(rw, r.WithContext(newRequestContext))
		})
	}
}
