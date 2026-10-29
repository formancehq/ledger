// Package httptransport gives an HTTP client a transport whose idle
// connections it may close without affecting other clients in the process.
package httptransport

import "net/http"

// CloneDefault returns a private copy of http.DefaultTransport and true.
//
// http.DefaultTransport is an assignable global: when something replaced it
// with a RoundTripper that is not an *http.Transport (a tracing or policy
// wrapper, for example), it cannot be cloned. CloneDefault then returns that
// shared RoundTripper and false; the caller must not close its idle
// connections, because they belong to every other user of the default.
func CloneDefault() (http.RoundTripper, bool) {
	return clone(http.DefaultTransport)
}

func clone(rt http.RoundTripper) (http.RoundTripper, bool) {
	if t, ok := rt.(*http.Transport); ok {
		return t.Clone(), true
	}

	return rt, false
}
