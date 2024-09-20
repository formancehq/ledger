package api

import (
	"net/http"

	"github.com/formancehq/go-libs/api"
	"github.com/pkg/errors"
)

func ReadOnly(h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet && r.Method != http.MethodOptions && r.Method != http.MethodHead {
			api.BadRequest(w, "READ_ONLY", errors.New("Read only mode"))
			return
		}
		h.ServeHTTP(w, r)
	})
}
