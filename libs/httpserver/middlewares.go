package httpserver

import (
	"net/http"
	"time"

	"github.com/formancehq/stack/libs/go-libs/logging"
)

func LoggerMiddleware(l logging.Logger) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()
			r = r.WithContext(logging.ContextWithLogger(r.Context(), l))

			// copy
			method := r.Method
			path := r.URL.Path

			h.ServeHTTP(w, r)

			latency := time.Since(start)

			l.WithFields(map[string]interface{}{
				"method":     method,
				"path":       path,
				"latency":    latency,
				"user_agent": r.UserAgent(),
			}).Info("Request")
		})
	}
}
