package http

import (
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
)

// IsLoopbackHost reports whether host, without a port, names the loopback
// interface.
func IsLoopbackHost(host string) bool {
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)

	return ip != nil && ip.IsLoopback()
}

// restoreRequestGuard rejects requests a web page can make. The restore routes
// have no authentication, and a browser on the same host can reach loopback:
// a loopback Host blocks DNS rebinding, a request with Origin or a non-"none"
// Sec-Fetch-Site comes from a page, and requiring JSON on POST makes every
// cross-origin POST need a preflight this router never grants.
func restoreRequestGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, _, err := net.SplitHostPort(r.Host)
		if err != nil {
			host = r.Host
		}
		if !IsLoopbackHost(host) {
			writeErrorResponse(w, http.StatusForbidden, "PERMISSION_DENIED",
				fmt.Errorf("restore routes require a loopback Host, got %q", r.Host))

			return
		}
		if site := r.Header.Get("Sec-Fetch-Site"); r.Header.Get("Origin") != "" || (site != "" && site != "none") {
			writeErrorResponse(w, http.StatusForbidden, "PERMISSION_DENIED",
				errors.New("restore routes do not accept browser-originated requests"))

			return
		}
		if r.Method == http.MethodPost {
			if mediaType, _, err := mime.ParseMediaType(r.Header.Get("Content-Type")); err != nil || mediaType != "application/json" {
				writeErrorResponse(w, http.StatusUnsupportedMediaType, "UNSUPPORTED_MEDIA_TYPE",
					errors.New("restore POST routes require Content-Type: application/json"))

				return
			}
		}

		next.ServeHTTP(w, r)
	})
}
