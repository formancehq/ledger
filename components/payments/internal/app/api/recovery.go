package api

import (
	"context"
	"net/http"
)

func recoveryHandler(reporter func(ctx context.Context, e interface{})) func(h http.Handler) http.Handler {
	return func(h http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if e := recover(); e != nil {
					w.WriteHeader(http.StatusInternalServerError)
					reporter(r.Context(), e)
				}
			}()
			h.ServeHTTP(w, r)
		})
	}
}
