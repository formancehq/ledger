package http

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net/http"
	"unicode/utf8"

	"github.com/go-chi/chi/v5"
)

// utf8BodyValidator is a middleware that rejects requests containing invalid UTF-8
// in the request body. This prevents invalid strings from reaching the protobuf
// layer, which requires valid UTF-8 for string fields.
// This runs as global middleware (before routing).
func utf8BodyValidator(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength != 0 && r.Body != nil {
			body, err := io.ReadAll(r.Body)
			if err != nil {
				writeBadRequest(w, "INVALID_REQUEST", fmt.Errorf("failed to read request body: %w", err))

				return
			}

			if !utf8.Valid(body) {
				writeBadRequest(w, "INVALID_REQUEST", errors.New("request body contains invalid UTF-8"))

				return
			}

			r.Body = io.NopCloser(bytes.NewReader(body))
		}

		next.ServeHTTP(w, r)
	})
}

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
