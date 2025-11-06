//go:build ee

package audit

import (
	"net/http"
)

// Middleware returns the HTTP audit middleware using our custom client
func Middleware(client *Client) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			client.AuditHTTPRequest(w, r, next)
		})
	}
}
