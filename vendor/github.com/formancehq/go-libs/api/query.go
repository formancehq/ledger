package api

import (
	"net/http"
	"strings"
)

func QueryParamBool(r *http.Request, key string) bool {
	v := strings.ToLower(r.URL.Query().Get(key))
	return v == "1" || v == "true"
}
