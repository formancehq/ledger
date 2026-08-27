package http

import (
	"context"
	"net/http"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/go-chi/chi/v5"

	"github.com/formancehq/ledger/v3/internal/query"
)

type requestClockKey struct{}

// startRequestClock stamps the instant the server starts handling a request, so
// a profiled read can measure from a point ABOVE its handler.
//
// It belongs immediately before internalauth.HTTPAuthMiddleware in the
// router-wide chain (see NewHandler). That middleware is where the expensive
// half of authentication lives — token extraction and JWT validation, a remote
// JWKS fetch on a cache miss — and it is router-wide, so it cannot be moved
// under a per-route profile. Stamping the time instead costs one time.Now() and
// one context value per request, on every route, and lets the four profiled
// reads start their clock where gRPC starts its own: after the transport
// middlewares, before authentication.
//
// Placing it here rather than first in the chain is what keeps the two
// transports comparable. gRPC calls query.WithProfile inside the handler, i.e.
// after its own interceptor chain (recovery, consistency, logging, error
// conversion); starting the HTTP clock at the very top would fold RequestID,
// otelhttp and the request logger into server_duration_us on HTTP only.
func startRequestClock(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := context.WithValue(r.Context(), requestClockKey{}, time.Now())

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// withQueryProfile installs a read-path timing profile (EN-1859) in the request
// context, backdated to the instant startRequestClock stamped so that
// authentication is inside the measured window.
//
// It must still sit outside the route's scope guard: the guard is the cheap half
// of authorization, but it is a real branch and it is inside the window on gRPC.
// chi applies group middleware before route middleware, so a profiled route
// cannot live inside a `r.With(requireXRead).Group(…)` block and get the ordering
// right — see the profiled-reads block in handler.go.
//
// Only the four profiled reads get this middleware. The presence of a profile in
// the context, not the caller's request for one, is what enables instrumentation
// (see query.WithProfile), so installing it on unprofiled routes would leave
// barrier waits accumulating into a record nobody reads.
func withQueryProfile(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, ok := r.Context().Value(requestClockKey{}).(time.Time)
		if !ok {
			// Impossible by contract: every profiled route is mounted under the
			// router-wide chain that stamps the clock (invariant #7). Falling back
			// to now still produces a coherent profile — it just under-reports
			// preparation by the authentication step, which is the defect this
			// middleware exists to close, so it must not pass unnoticed.
			assert.Unreachable("profiled HTTP read has no request clock — startRequestClock is missing from the router chain", map[string]any{
				"method": r.Method,
			})

			start = time.Now()
		}

		ctx, _ := query.WithProfileStartingAt(r.Context(), start)

		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// profileFromRequest returns the profile withQueryProfile installed for this
// route.
//
// Reaching a profiled handler without one means the route was registered outside
// the profiled block, which is impossible by contract — so it is asserted rather
// than tolerated silently (invariant #7). A read request is the wrong place to
// fail hard over a diagnostic: every QueryProfile method is nil-safe, so the
// request still serves and the only consequence is an unprofiled response.
func profileFromRequest(r *http.Request) *query.QueryProfile {
	profile := query.ProfileFromContext(r.Context())
	if profile == nil {
		// Not r.URL.Path: the assertion catalogue must not be keyed on a value
		// carrying a ledger name. RouteContext is nil when the handler is called
		// outside the router at all, which is how the unit tests invoke it.
		pattern := "<no route context>"
		if rc := chi.RouteContext(r.Context()); rc != nil {
			pattern = rc.RoutePattern()
		}

		assert.Unreachable("profiled HTTP read reached its handler with no query profile — route registered outside the profiled block", map[string]any{
			"method":  r.Method,
			"pattern": pattern,
		})
	}

	return profile
}
