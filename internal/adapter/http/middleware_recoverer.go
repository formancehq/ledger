package http

import (
	"fmt"
	"net/http"
	"runtime/debug"
)

// jsonRecoverer is a middleware that recovers from panics and returns a JSON
// error response instead of the default text/plain from Chi's Recoverer.
// This ensures all responses (including panic recovery) use application/json
// content type for OpenAPI conformance.
func jsonRecoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rvr := recover(); rvr != nil {
				if rvr == http.ErrAbortHandler { //nolint:errorlint // rvr is interface{}, not error
					// Respect ErrAbortHandler - don't recover, let the server handle it.
					panic(rvr)
				}

				debug.PrintStack()

				writeErrorResponse(
					w,
					http.StatusInternalServerError,
					"INTERNAL_ERROR",
					fmt.Errorf("panic recovered: %v", rvr),
				)
			}
		}()

		next.ServeHTTP(w, r)
	})
}
