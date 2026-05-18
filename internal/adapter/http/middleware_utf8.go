package http

import (
	"fmt"
	"net/http"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
)

// utf8PathParamValidator is a middleware that rejects requests containing invalid
// UTF-8 in URL path parameters. Chi URL-decodes percent-encoded path segments,
// which can produce invalid UTF-8 from encoded byte sequences.
// This must run after routing (inside a route group) so chi params are available.
func utf8PathParamValidator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if rctx := chi.RouteContext(r.Context()); rctx != nil {
			for i, val := range rctx.URLParams.Values {
				if !utf8.ValidString(val) {
					key := ""
					if i < len(rctx.URLParams.Keys) {
						key = rctx.URLParams.Keys[i]
					}

					writeBadRequest(w, "INVALID_REQUEST",
						fmt.Errorf("path parameter %q contains invalid UTF-8", key))

					return
				}
			}
		}

		next.ServeHTTP(w, r)
	})
}
