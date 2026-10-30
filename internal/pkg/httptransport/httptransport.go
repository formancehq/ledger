// Package httptransport gives an HTTP client a transport whose idle
// connections it may close without affecting other clients in the process.
package httptransport

import "net/http"

// Clone returns a private copy of rt and true when rt is an *http.Transport.
//
// Callers pass http.DefaultTransport, which is an assignable global: when
// something replaced it with another RoundTripper (a tracing or policy
// wrapper, for example), it cannot be cloned. Clone then returns rt itself and
// false; the caller shares it and must not close its idle connections, because
// they belong to every other user of the default.
func Clone(rt http.RoundTripper) (http.RoundTripper, bool) {
	if t, ok := rt.(*http.Transport); ok {
		return t.Clone(), true
	}

	return rt, false
}
