package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"reflect"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/query"
)

func TestStartRequestClock_StampsAnInstant(t *testing.T) {
	t.Parallel()

	var stamped any

	startRequestClock(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		stamped = r.Context().Value(requestClockKey{})
	})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	at, ok := stamped.(time.Time)
	require.True(t, ok, "the clock must be stamped as a time.Time")
	assert.False(t, at.IsZero())
}

// The point of splitting the clock from the profile is that the profile measures
// from ABOVE its own middleware. Proven by backdating the stamp: if
// withQueryProfile ignored it and called time.Now(), the reported total would be
// microseconds instead of the planted hour.
func TestWithQueryProfile_MeasuresFromTheStampedClock(t *testing.T) {
	t.Parallel()

	var seen *query.QueryProfile

	handler := withQueryProfile(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = query.ProfileFromContext(r.Context())
	}))

	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r = r.WithContext(context.WithValue(r.Context(), requestClockKey{}, time.Now().Add(-time.Hour)))

	handler.ServeHTTP(httptest.NewRecorder(), r)

	require.NotNil(t, seen, "the middleware must install a profile for downstream handlers")

	seen.Finish()
	assert.GreaterOrEqual(t, seen.ServerDuration, time.Hour,
		"the profile must measure from the stamped clock, not from its own middleware")
}

// A missing clock is a router misconfiguration, not a request-time failure: the
// read still serves, with preparation under-reported by the authentication step.
func TestWithQueryProfile_ToleratesAMissingClock(t *testing.T) {
	t.Parallel()

	var seen *query.QueryProfile

	withQueryProfile(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		seen = query.ProfileFromContext(r.Context())
	})).ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/", nil))

	require.NotNil(t, seen)

	seen.Finish()
	assert.Positive(t, seen.ServerDuration, "the fallback clock must still produce a usable profile")
}

// profiledRoutes is the closed set of reads that carry a query profile. Kept here
// rather than derived from the router, so that a route silently joining or
// leaving the set fails a test.
var profiledRoutes = []struct{ method, pattern string }{
	{http.MethodGet, "/v3/{ledgerName}/transactions"},
	{http.MethodGet, "/v3/{ledgerName}/accounts"},
	{http.MethodGet, "/v3/{ledgerName}/volumes"},
	{http.MethodPost, "/v3/{ledgerName}/prepared-queries/{queryName}/execute"},
}

// This pins the ordering that makes the two transports comparable. gRPC calls
// query.WithProfile before internalauth.Authenticate, so authentication lands in
// prepare_duration_us there; on HTTP authentication is router-wide, above the
// handler, so the clock has to be stamped before it and the profile has to
// adopt that stamp. Get the order wrong and the same field name means a
// different span on each transport.
//
// Asserted structurally because the consequence of the wrong order is a duration
// that is merely *smaller*: no assertion on a duration can distinguish "auth was
// fast" from "auth was not measured".
func TestProfiledReadRoutes_ClockPrecedesAuthPrecedesScopeGuard(t *testing.T) {
	t.Parallel()

	chains := walkRouteMiddlewares(t)

	for _, route := range profiledRoutes {
		key := route.method + " " + route.pattern

		chain, ok := chains[key]
		require.True(t, ok, "route %s is not registered — the profiled set drifted from the router", key)

		clockAt := indexOfMiddleware(chain, "startRequestClock")
		authAt := indexOfMiddleware(chain, "HTTPAuthMiddleware")
		profileAt := indexOfMiddleware(chain, "withQueryProfile")
		scopeAt := indexOfMiddleware(chain, "RequireScope")

		require.GreaterOrEqual(t, clockAt, 0, "route %s does not stamp a request clock", key)
		require.GreaterOrEqual(t, authAt, 0, "route %s does not authenticate", key)
		require.GreaterOrEqual(t, profileAt, 0, "route %s has no query-profile middleware", key)
		require.GreaterOrEqual(t, scopeAt, 0, "route %s has no scope guard", key)

		assert.Less(t, clockAt, authAt,
			"route %s must stamp its clock before authenticating, otherwise JWT validation is excluded from prepare_duration_us", key)
		assert.Less(t, profileAt, scopeAt,
			"route %s must install its profile before the scope guard, which is inside the measured window on gRPC", key)
	}
}

// The profile is what enables instrumentation, so installing it on a route that
// never reports one would leave barrier waits accumulating into a record nobody
// consumes. This pins the set closed from the other side.
func TestUnprofiledRoutes_HaveNoQueryProfile(t *testing.T) {
	t.Parallel()

	profiled := map[string]bool{}
	for _, route := range profiledRoutes {
		profiled[route.method+" "+route.pattern] = true
	}

	for key, chain := range walkRouteMiddlewares(t) {
		if profiled[key] {
			continue
		}

		assert.Equal(t, -1, indexOfMiddleware(chain, "withQueryProfile"),
			"route %s carries a query profile but never reports one", key)
	}
}

// indexOfMiddleware locates a middleware by the name of the function that
// produced it, returning -1 when absent.
//
// By name and not by code pointer: the guards are closures returned by
// RequireScope, which the compiler instantiates per call site, so
// reflect.Pointer differs between the router's guard and one built in a test.
// The produced names ("…NewHandler.RequireScope.func7") still carry the
// constructor, which is the identity that matters here.
func indexOfMiddleware(chain []func(http.Handler) http.Handler, name string) int {
	for i, mw := range chain {
		fn := runtime.FuncForPC(reflect.ValueOf(mw).Pointer())
		if fn != nil && strings.Contains(fn.Name(), name) {
			return i
		}
	}

	return -1
}

// walkRouteMiddlewares maps "METHOD pattern" to the middleware chain the real
// router applies, router-wide and group middleware included, in application
// order. Route-level `r.With(…).Get(…)` middleware is invisible to chi.Walk,
// which is why the profiled block nests groups instead.
func walkRouteMiddlewares(t *testing.T) map[string][]func(http.Handler) http.Handler {
	t.Helper()

	handler := NewHandler(logging.Testing(), nil, internalauth.AuthConfig{}, version.Info{})

	routes, ok := handler.(chi.Routes)
	require.True(t, ok, "NewHandler must return a walkable chi router")

	chains := map[string][]func(http.Handler) http.Handler{}

	require.NoError(t, chi.Walk(routes, func(method, route string, _ http.Handler, middlewares ...func(http.Handler) http.Handler) error {
		chains[method+" "+route] = middlewares

		return nil
	}))

	require.NotEmpty(t, chains, "the walk found no routes at all")

	return chains
}
