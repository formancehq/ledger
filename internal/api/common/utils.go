package common

import (
	"encoding/json"
	"github.com/formancehq/go-libs/v3/api"
	"net/http"
)

func WithBody[V any](w http.ResponseWriter, r *http.Request, fn func(v V)) {
	var v V
	if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
		api.BadRequest(w, "VALIDATION", err)
		return
	}

	fn(v)
}
