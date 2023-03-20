package middlewares

import (
	"net/http"
	"time"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/stack/libs/go-libs/logging"
)

func Log() func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := core.Now()
			h.ServeHTTP(w, r)
			latency := time.Since(start.Time)
			logging.FromContext(r.Context()).WithFields(map[string]interface{}{
				"method":     r.Method,
				"path":       r.URL.Path,
				"latency":    latency,
				"user_agent": r.UserAgent(),
			}).Info("Request")
		})
	}
}
