package http

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/adapter/auth/authtest"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

const (
	// testAuthIssuer is the issuer both the router config and every minted token
	// declare, so validateToken accepts them.
	testAuthIssuer = "https://issuer.test"

	// probeBody is the request body every probe sends. The scope gate runs before
	// any body parsing, so the payload only has to be syntactically plausible:
	// what a handler makes of it afterwards is not what these tests assert.
	probeBody = "{}"

	// emptyBatchBody is the bulk payload used by the per-element probe: a batch
	// with no element to authorize.
	emptyBatchBody = "[]"
)

// routeKind classifies how a route is authorized. It is an explicit field
// rather than "an empty scope means public", because public-by-omission of a
// RequireScope is exactly the defect class this test exists to catch: a row has
// to state which of the three it is.
type routeKind int

const (
	// kindGuarded: exactly one granular scope opens the route.
	kindGuarded routeKind = iota
	// kindPublic: no scope gate at all, reachable without a token, by design.
	kindPublic
	// kindPerElement: no route-level gate; the handler authorizes each element.
	kindPerElement
)

// routeScope is one row of the declared contract: the scope a given route
// requires. The dedicated route is the source of truth, mirroring how
// requestScopeCases documents the gRPC side
// (internal/adapter/auth/request_scope_exhaustiveness_test.go:109).
type routeScope struct {
	method    string // HTTP method; "*" when anyMethod is set
	pattern   string // exactly as chi.Walk reports it
	kind      routeKind
	scope     internalauth.Scope // meaningful only when kind == kindGuarded
	anyMethod bool               // registered with r.Handle, so answers all methods
}

// anyMethodMethods is the method set chi fans a r.Handle registration out to
// (see chi's methodMap). An anyMethod row expands to one walk entry per method.
var anyMethodMethods = []string{
	http.MethodConnect, http.MethodDelete, http.MethodGet, http.MethodHead,
	http.MethodOptions, http.MethodPatch, http.MethodPost, http.MethodPut,
	http.MethodTrace,
}

// expectedRouteScopes is the declared route-to-scope contract for the REST
// router: one row per registered endpoint, anyMethod rows standing in for the
// method fan-out chi performs.
//
// Adding a route to handler.go without adding a row here fails
// TestRouteScopes_Exhaustive. That is the point: the scope a route requires is
// an authorization decision, and it must be made explicitly rather than
// inherited from whichever Group block happens to enclose the line.
//
// The declared scope is not documentation either:
// TestRouteScopes_GuardedRoutesRequireExactlyOneScope drives a real request per
// row per granular scope, so a row naming the wrong scope fails.
func expectedRouteScopes() []routeScope {
	return []routeScope{
		// --- Unversioned ops routes: no scope gate, public by design ---
		//
		// NOTE on /clusterz: it is public by OMISSION of a RequireScope, and it
		// is absent from HTTPAuthMiddleware's exempt list (http_middleware.go:40)
		// unlike /health, /livez and /readyz. So a tokenless request reaches the
		// handler while an *invalid* token yields 401 — an asymmetry against the
		// documented behaviour in docs/ops/authentication.md:94. Pinned as-is
		// here; see the EN-1775 follow-up.
		{http.MethodGet, "/health", kindPublic, "", false},
		{http.MethodGet, "/livez", kindPublic, "", false},
		{http.MethodGet, "/readyz", kindPublic, "", false},
		{http.MethodGet, "/clusterz", kindPublic, "", false},
		{http.MethodGet, "/_info", kindPublic, "", false},

		// --- pprof, requireOpsRead (handler.go:77-89) ---
		{http.MethodGet, "/debug/pprof/", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/debug/pprof/cmdline", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/debug/pprof/profile", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/debug/pprof/symbol", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/debug/pprof/trace", kindGuarded, internalauth.ScopeOpsRead, false},
		// These six use r.Handle, so they answer all 9 methods. anyMethod
		// declares that fan-out instead of hiding it behind 54 rows. If they
		// ever become r.Get, drop the flag and the table diff records it.
		{"*", "/debug/pprof/allocs", kindGuarded, internalauth.ScopeOpsRead, true},
		{"*", "/debug/pprof/block", kindGuarded, internalauth.ScopeOpsRead, true},
		{"*", "/debug/pprof/goroutine", kindGuarded, internalauth.ScopeOpsRead, true},
		{"*", "/debug/pprof/heap", kindGuarded, internalauth.ScopeOpsRead, true},
		{"*", "/debug/pprof/mutex", kindGuarded, internalauth.ScopeOpsRead, true},
		{"*", "/debug/pprof/threadcreate", kindGuarded, internalauth.ScopeOpsRead, true},

		// --- Ledgers read, requireLedgersRead (handler.go:104-117) ---
		{http.MethodGet, "/v3/", kindGuarded, internalauth.ScopeLedgersRead, false},
		{http.MethodGet, "/v3/{ledgerName}", kindGuarded, internalauth.ScopeLedgersRead, false},
		{http.MethodGet, "/v3/{ledgerName}/stats", kindGuarded, internalauth.ScopeLedgersRead, false},
		{http.MethodGet, "/v3/{ledgerName}/logs", kindGuarded, internalauth.ScopeLedgersRead, false},
		{http.MethodGet, "/v3/{ledgerName}/numscripts", kindGuarded, internalauth.ScopeLedgersRead, false},
		{http.MethodGet, "/v3/{ledgerName}/numscripts/{name}", kindGuarded, internalauth.ScopeLedgersRead, false},
		{http.MethodGet, "/v3/{ledgerName}/numscripts/{name}/usage", kindGuarded, internalauth.ScopeLedgersRead, false},
		{http.MethodGet, "/v3/{ledgerName}/numscripts/{name}/versions", kindGuarded, internalauth.ScopeLedgersRead, false},
		{http.MethodGet, "/v3/{ledgerName}/indexes", kindGuarded, internalauth.ScopeLedgersRead, false},
		{http.MethodGet, "/v3/{ledgerName}/indexes/{canonicalId}", kindGuarded, internalauth.ScopeLedgersRead, false},
		{http.MethodGet, "/v3/{ledgerName}/indexes/{canonicalId}/status", kindGuarded, internalauth.ScopeLedgersRead, false},
		{http.MethodGet, "/v3/{ledgerName}/indexes/{canonicalId}/inspect", kindGuarded, internalauth.ScopeLedgersRead, false},

		// --- Transactions read, requireTransactionsRead (handler.go:120-123) ---
		{http.MethodGet, "/v3/{ledgerName}/transactions", kindGuarded, internalauth.ScopeTransactionsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/transactions/{transactionId}", kindGuarded, internalauth.ScopeTransactionsRead, false},

		// --- Audit read, requireAuditRead (handler.go:132-135) ---
		{http.MethodGet, "/v3/_/audit-entries", kindGuarded, internalauth.ScopeAuditRead, false},
		{http.MethodGet, "/v3/_/audit-entries/{sequence}", kindGuarded, internalauth.ScopeAuditRead, false},

		// --- Ops read, requireOpsRead, bucket-wide /_ subtree (handler.go:147-157) ---
		{http.MethodGet, "/v3/_/logs/{sequence}", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/chapters", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/chapter-schedule", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/events-sinks", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/signing-keys", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/indexes", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/indexes/status", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/indexes/{canonicalId}", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/indexes/{canonicalId}/status", kindGuarded, internalauth.ScopeOpsRead, false},

		// --- Accounts read, requireAccountsRead (handler.go:160-169) ---
		{http.MethodGet, "/v3/{ledgerName}/accounts", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/accounts/{address}", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/volumes", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/metadata-schema", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/analyze-accounts", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/analyze-transactions", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/account-types", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/account-types/{typeName}", kindGuarded, internalauth.ScopeAccountsRead, false},

		// --- Ledgers write, requireLedgersWrite (handler.go:172-179) ---
		{http.MethodPost, "/v3/{ledgerName}", kindGuarded, internalauth.ScopeLedgersWrite, false},
		{http.MethodDelete, "/v3/{ledgerName}", kindGuarded, internalauth.ScopeLedgersWrite, false},
		{http.MethodPost, "/v3/{ledgerName}/promote", kindGuarded, internalauth.ScopeLedgersWrite, false},
		{http.MethodPut, "/v3/{ledgerName}/numscripts/{name}", kindGuarded, internalauth.ScopeLedgersWrite, false},
		{http.MethodPost, "/v3/{ledgerName}/indexes", kindGuarded, internalauth.ScopeLedgersWrite, false},
		{http.MethodDelete, "/v3/{ledgerName}/indexes/{canonicalId}", kindGuarded, internalauth.ScopeLedgersWrite, false},

		// --- Transactions write, requireTransactionsWrite (handler.go:182-185) ---
		{http.MethodPost, "/v3/{ledgerName}/transactions", kindGuarded, internalauth.ScopeTransactionsWrite, false},
		{http.MethodPost, "/v3/{ledgerName}/transactions/{transactionId}/revert", kindGuarded, internalauth.ScopeTransactionsWrite, false},

		// --- Metadata write, requireMetadataWrite (handler.go:188-200) ---
		{http.MethodPost, "/v3/{ledgerName}/transactions/{transactionId}/metadata", kindGuarded, internalauth.ScopeMetadataWrite, false},
		{http.MethodDelete, "/v3/{ledgerName}/transactions/{transactionId}/metadata/{key}", kindGuarded, internalauth.ScopeMetadataWrite, false},
		{http.MethodPost, "/v3/{ledgerName}/accounts/{address}/metadata", kindGuarded, internalauth.ScopeMetadataWrite, false},
		{http.MethodDelete, "/v3/{ledgerName}/accounts/{address}/metadata/{key}", kindGuarded, internalauth.ScopeMetadataWrite, false},
		{http.MethodPost, "/v3/{ledgerName}/metadata", kindGuarded, internalauth.ScopeMetadataWrite, false},
		{http.MethodDelete, "/v3/{ledgerName}/metadata/{key}", kindGuarded, internalauth.ScopeMetadataWrite, false},
		{http.MethodPut, "/v3/{ledgerName}/metadata-schema/{targetType}/{key}", kindGuarded, internalauth.ScopeMetadataWrite, false},
		{http.MethodDelete, "/v3/{ledgerName}/metadata-schema/{targetType}/{key}", kindGuarded, internalauth.ScopeMetadataWrite, false},
		{http.MethodPost, "/v3/{ledgerName}/account-types", kindGuarded, internalauth.ScopeMetadataWrite, false},
		{http.MethodDelete, "/v3/{ledgerName}/account-types/{typeName}", kindGuarded, internalauth.ScopeMetadataWrite, false},
		{http.MethodPut, "/v3/{ledgerName}/account-types/default-enforcement-mode", kindGuarded, internalauth.ScopeMetadataWrite, false},

		// --- Bulk: no route-level gate; handleBulk checks each element
		// (handler.go:203, handlers_bulk.go:64) ---
		{http.MethodPost, "/v3/{ledgerName}/bulk", kindPerElement, "", false},

		// --- Prepared queries read, requireQueriesRead (handler.go:206-209) ---
		{http.MethodGet, "/v3/{ledgerName}/prepared-queries", kindGuarded, internalauth.ScopeQueriesRead, false},
		{http.MethodPost, "/v3/{ledgerName}/prepared-queries/{queryName}/execute", kindGuarded, internalauth.ScopeQueriesRead, false},

		// --- Prepared queries write, requireQueriesWrite (handler.go:212-216) ---
		{http.MethodPost, "/v3/{ledgerName}/prepared-queries", kindGuarded, internalauth.ScopeQueriesWrite, false},
		{http.MethodPut, "/v3/{ledgerName}/prepared-queries/{queryName}", kindGuarded, internalauth.ScopeQueriesWrite, false},
		{http.MethodDelete, "/v3/{ledgerName}/prepared-queries/{queryName}", kindGuarded, internalauth.ScopeQueriesWrite, false},
	}
}

// walkKey identifies one routed endpoint.
type walkKey struct {
	method  string
	pattern string
}

// inertBackend returns a Backend that is deliberately not stubbed, and
// deliberately NOT bound to the test's *testing.T.
//
// The probes assert only the authorization outcome, so what a handler does
// after the gate is irrelevant. The mock controller reports through
// panickingReporter rather than through t: there is no Finish, and an
// unexpected call unwinds until jsonRecoverer turns it into a 500 — neither 401
// nor 403 — instead of failing the test. The alternative, stubbing all ~30
// Controller methods with AnyTimes, would make these tests break every time
// Controller gains a method, which is the opposite of what a stable
// authorization guard should do.
func inertBackend() *MockBackend {
	return NewMockBackend(gomock.NewController(panickingReporter{}))
}

// panickingReporter converts gomock's failure reports into a panic: Errorf is
// dropped, and the Fatalf that an unexpected call raises becomes a panic the
// router's jsonRecoverer turns into the 500 described on inertBackend.
type panickingReporter struct{}

func (panickingReporter) Errorf(_ string, _ ...any) {}

func (panickingReporter) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// newScopedHandler builds the real router with authentication enabled.
func newScopedHandler(t *testing.T, keySet oidc.KeySet) http.Handler {
	t.Helper()

	return NewHandler(logging.Testing(), inertBackend(), internalauth.AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		Issuer:       testAuthIssuer,
		Service:      "ledger",
		ScopeMapping: internalauth.DefaultMapping("ledger"),
	}, version.Info{})
}

// walkRoutes enumerates every (method, pattern) the router registers.
func walkRoutes(t *testing.T, handler http.Handler) map[walkKey]struct{} {
	t.Helper()

	routes, ok := handler.(chi.Routes)
	require.True(t, ok, "NewHandler must return a chi router so the route table can be walked")

	found := map[walkKey]struct{}{}

	err := chi.Walk(routes, func(method, pattern string, _ http.Handler, _ ...func(http.Handler) http.Handler) error {
		found[walkKey{method, pattern}] = struct{}{}

		return nil
	})
	require.NoError(t, err)

	return found
}

// expandTable expands anyMethod rows into one entry per HTTP method, and fails
// the test if two rows claim the same endpoint.
//
// It takes t rather than returning the duplicates for the caller to assert on,
// so no call site can forget the check: the map it returns is keyed by
// (method, pattern), so a second row for an endpoint silently overwrites the
// first and BOTH reconciliation directions stay green while one of the two
// declared scopes is never probed.
func expandTable(t *testing.T, rows []routeScope) map[walkKey]routeScope {
	t.Helper()

	expanded := map[walkKey]routeScope{}

	var duplicates []string

	claim := func(key walkKey, row routeScope) {
		if _, taken := expanded[key]; taken {
			duplicates = append(duplicates, fmt.Sprintf("%s %s", key.method, key.pattern))

			return
		}

		expanded[key] = row
	}

	for _, row := range rows {
		if !row.anyMethod {
			claim(walkKey{row.method, row.pattern}, row)

			continue
		}

		for _, method := range anyMethodMethods {
			claim(walkKey{method, row.pattern}, row)
		}
	}

	slices.Sort(duplicates)
	require.Empty(t, duplicates,
		"expectedRouteScopes declares these endpoints more than once — keep exactly one row per "+
			"endpoint, or the duplicate's scope is never probed: %s",
		strings.Join(duplicates, ", "))

	return expanded
}

// TestRouteScopes_Exhaustive reconciles the declared table against the routes
// the router actually registers, in both directions. This is the HTTP analogue
// of TestRequiredScopeForRequest_ProtoExhaustive
// (internal/adapter/auth/request_scope_exhaustiveness_test.go:33).
func TestRouteScopes_Exhaustive(t *testing.T) {
	t.Parallel()

	_, keySet := authtest.KeyPair(t)
	walked := walkRoutes(t, newScopedHandler(t, keySet))
	expanded := expandTable(t, expectedRouteScopes())

	// Both directions collect every offender and report them sorted, rather than
	// stopping at the first: map iteration is randomized, so a FailNow per
	// mismatch would name a different route on each run and read as flakiness.
	var undeclared []string

	for key := range walked {
		if _, ok := expanded[key]; !ok {
			undeclared = append(undeclared, fmt.Sprintf("%s %s", key.method, key.pattern))
		}
	}

	slices.Sort(undeclared)
	// assert, not require: the stale-row direction below is independent and its
	// findings are just as useful, so one bad edit reports both halves at once.
	assert.Empty(t, undeclared,
		"these endpoints are registered by handler.go but have no row in expectedRouteScopes — "+
			"add one declaring the granular scope each requires (or kindPublic / kindPerElement), "+
			"so the authorization decision is explicit: %s",
		strings.Join(undeclared, ", "))

	var stale []string

	for key := range expanded {
		if _, ok := walked[key]; !ok {
			stale = append(stale, fmt.Sprintf("%s %s", key.method, key.pattern))
		}
	}

	slices.Sort(stale)
	require.Empty(t, stale,
		"expectedRouteScopes declares these endpoints which the router no longer registers — "+
			"remove the rows: %s",
		strings.Join(stale, ", "))
}

// TestRouteScopes_TableComposition checks the declared table is internally
// well-formed, so the reconciliation and the probes actually cover every row:
// no endpoint declared twice, the anyMethod fan-out expanding to the arithmetic
// it claims, and a scope declared exactly on the rows that have a gate.
func TestRouteScopes_TableComposition(t *testing.T) {
	t.Parallel()

	rows := expectedRouteScopes()

	// expandTable itself fails on a duplicated endpoint.
	expanded := expandTable(t, rows)

	var anyMethodRows, singleMethodRows int

	for _, row := range rows {
		if row.anyMethod {
			anyMethodRows++
		} else {
			singleMethodRows++
		}
	}

	// Derived, not frozen: adding a route changes the operands, so the identity
	// still holds and nobody has to bump a magic number.
	require.Len(t, expanded, singleMethodRows+len(anyMethodMethods)*anyMethodRows,
		"expanding %d single-method rows and %d anyMethod rows must yield one entry per endpoint",
		singleMethodRows, anyMethodRows)

	// A scope is declared exactly on the guarded rows. Both directions are
	// load-bearing: a kindGuarded row with an empty scope would make the positive
	// probe blame the router for a malformed row, and a scope on a kindPublic or
	// kindPerElement row is read by nothing at all, so it would drift unnoticed.
	var malformed []string

	for _, row := range rows {
		switch {
		case row.kind == kindGuarded && row.scope == "":
			malformed = append(malformed, fmt.Sprintf(
				"%s %s is kindGuarded but declares no scope", row.method, row.pattern))
		case row.kind != kindGuarded && row.scope != "":
			malformed = append(malformed, fmt.Sprintf(
				"%s %s is not kindGuarded but declares scope %s", row.method, row.pattern, row.scope))
		}
	}

	slices.Sort(malformed)
	require.Empty(t, malformed,
		"a row must declare a scope if and only if it is kindGuarded: %s",
		strings.Join(malformed, "; "))
}

// pathParamValues instantiates chi patterns with concrete, plain-ASCII values.
// ASCII matters: utf8PathParamValidator runs before RequireScope
// (handler.go:102), so a value it rejects would short-circuit with 400 and hide
// the scope decision under test.
var pathParamValues = map[string]string{
	"{ledgerName}":    "main",
	"{transactionId}": "1",
	"{sequence}":      "1",
	"{canonicalId}":   "idx",
	"{address}":       "accounts:1",
	"{key}":           "k",
	"{targetType}":    "transaction",
	"{typeName}":      "asset",
	"{name}":          "script",
	"{queryName}":     "q",
}

// concreteTarget turns a chi pattern into a request target. The pattern is used
// verbatim apart from its parameters — including the trailing slash on "/v3/" —
// so a probe hits the very endpoint chi.Walk reported.
func concreteTarget(t *testing.T, pattern string) string {
	t.Helper()

	target := pattern
	for placeholder, value := range pathParamValues {
		target = strings.ReplaceAll(target, placeholder, value)
	}

	require.NotContains(t, target, "{",
		"pattern %s has a path parameter with no entry in pathParamValues — add one", pattern)

	return target
}

// probeRequest describes one end-to-end request through the real middleware
// chain.
type probeRequest struct {
	handler http.Handler
	method  string
	pattern string
	token   string // empty means no Authorization header is sent
	body    string
}

// probe drives one request through the real middleware chain and returns the
// status.
//
// End-to-end requests are the only sound oracle for the scope a route requires.
// The middleware slice chi.Walk exposes is not one: `With(mw).Route(...)`
// attaches mw to the subrouter's handler rather than to Middlewares(), so
// /v3/_/chapters shows no RequireScope yet answers 401 tokenless — and all ten
// RequireScope closures share a single code pointer anyway.
func probe(t *testing.T, in probeRequest) int {
	t.Helper()

	req := httptest.NewRequest(in.method, concreteTarget(t, in.pattern), strings.NewReader(in.body))
	req.Header.Set("Content-Type", "application/json")

	if in.token != "" {
		req.Header.Set("Authorization", "Bearer "+in.token)
	}

	req = req.WithContext(cancelledProbeContext())

	w := httptest.NewRecorder()
	in.handler.ServeHTTP(w, req)

	return w.Code
}

// cancelledProbeContext returns an already-cancelled context for probe requests.
//
// It exists for /debug/pprof/profile and /debug/pprof/trace: once the OpsRead
// gate admits the positive probe, net/http/pprof really runs, and its sleep
// helper blocks for 30s and 1s respectively unless the request context is done
// (seconds=0 is not accepted — the handler falls back to the default). It is
// applied to every probe rather than to those two patterns alone so there is one
// rule to reason about, and it cannot weaken the oracle: no middleware in the
// chain consults the context, and the negative probes assert an exact 403
// through this same context, which is only reachable by running RequireScope.
func cancelledProbeContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	return ctx
}

// probeMethod is the method to send for a row: anyMethod rows share one handler
// and one middleware chain across all 9 methods, so one representative method
// asserts everything the other 8 would.
func (r routeScope) probeMethod() string {
	if r.anyMethod {
		return http.MethodGet
	}

	return r.method
}

// grantedScopes returns every granular scope in a deterministic order.
// internalauth.AllGranularScopes is a map, so probing it directly would name a
// different offending scope on each run and read as flakiness.
func grantedScopes() []internalauth.Scope {
	scopes := make([]internalauth.Scope, 0, len(internalauth.AllGranularScopes))
	for scope := range internalauth.AllGranularScopes {
		scopes = append(scopes, scope)
	}

	slices.Sort(scopes)

	return scopes
}

// scopeProbeFixture is the per-test setup shared by the probes: the real router
// plus one bearer token per granular scope.
//
// One RSA key pair and one token per scope are minted per test, never inside the
// per-route loop: authtest.KeyPair generates a 2048-bit key, which would
// otherwise dominate the runtime of a route × scope sweep.
type scopeProbeFixture struct {
	handler http.Handler
	tokens  map[internalauth.Scope]string
}

func newScopeProbeFixture(t *testing.T) scopeProbeFixture {
	t.Helper()

	key, keySet := authtest.KeyPair(t)

	tokens := make(map[internalauth.Scope]string, len(internalauth.AllGranularScopes))

	for _, scope := range grantedScopes() {
		// A granular scope is not a key of DefaultMapping, so it reaches the
		// gate through ExpandScopes' identity pass-through: exactly one granular
		// scope per token, never an aggregate that would expand to several.
		tokens[scope] = authtest.SignToken(t, key, authtest.Claims(testAuthIssuer, string(scope)))
	}

	return scopeProbeFixture{handler: newScopedHandler(t, keySet), tokens: tokens}
}

// TestRouteScopes_GuardedRoutesRequireExactlyOneScope drives a real request per
// guarded route per granular scope: the declared scope must be admitted, and
// every other granular scope must be refused with 403.
//
// Probing every non-declared scope is what turns the declared table from
// documentation into a proof that *exactly one* scope opens each route. It also
// covers the EN-1508 LedgerRead/OpsRead confusion by construction, rather than
// relying on someone picking the right foil.
//
// Only granular scopes are used, never the aggregate "ledger:read": that one
// expands to both ScopeLedgersRead and ScopeOpsRead (scopes.go:70-77) and would
// mask exactly this drift.
func TestRouteScopes_GuardedRoutesRequireExactlyOneScope(t *testing.T) {
	t.Parallel()

	fixture := newScopeProbeFixture(t)
	scopes := grantedScopes()

	for _, row := range expectedRouteScopes() {
		if row.kind != kindGuarded {
			continue
		}

		method := row.probeMethod()

		admitted := probe(t, probeRequest{
			handler: fixture.handler,
			method:  method,
			pattern: row.pattern,
			token:   fixture.tokens[row.scope],
			body:    probeBody,
		})

		// Anything but 401/403: past the gate, the unstubbed backend decides the
		// status (see inertBackend), and that is not what this asserts.
		assert.NotEqual(t, http.StatusUnauthorized, admitted,
			"%s %s must admit its declared scope %s but answered 401", method, row.pattern, row.scope)
		assert.NotEqual(t, http.StatusForbidden, admitted,
			"%s %s must admit its declared scope %s but answered 403", method, row.pattern, row.scope)

		for _, foil := range scopes {
			if foil == row.scope {
				continue
			}

			refused := probe(t, probeRequest{
				handler: fixture.handler,
				method:  method,
				pattern: row.pattern,
				token:   fixture.tokens[foil],
				body:    probeBody,
			})

			// A token was presented, so the gate answers 403, never 401. Any other
			// status means either the route is open to a scope it must not accept,
			// or a middleware ahead of the gate answered first.
			assert.Equal(t, http.StatusForbidden, refused,
				"%s %s must refuse scope %s — only %s opens it — but answered %d",
				method, row.pattern, foil, row.scope, refused)
		}
	}
}

// TestRouteScopes_PublicRoutesNeedNoToken checks the kindPublic rows are
// reachable with no Authorization header at all.
func TestRouteScopes_PublicRoutesNeedNoToken(t *testing.T) {
	t.Parallel()

	fixture := newScopeProbeFixture(t)

	probed := 0

	for _, row := range expectedRouteScopes() {
		if row.kind != kindPublic {
			continue
		}

		probed++

		status := probe(t, probeRequest{
			handler: fixture.handler,
			method:  row.probeMethod(),
			pattern: row.pattern,
			body:    probeBody,
		})

		assert.NotEqual(t, http.StatusUnauthorized, status,
			"%s %s is declared kindPublic but answered 401 without a token", row.method, row.pattern)
		assert.NotEqual(t, http.StatusForbidden, status,
			"%s %s is declared kindPublic but answered 403 without a token", row.method, row.pattern)
	}

	require.NotZero(t, probed, "no kindPublic row left to probe — the public contract went unverified")
}

// TestRouteScopes_PerElementRoutesHaveNoRouteGate checks the kindPerElement rows
// carry no route-level gate.
//
// The foil is a token carrying ScopeClusterRead, which opens no route in the
// declared table, so a route-level RequireScope would answer 403. Reaching the
// handler with an empty batch — which has no element to authorize
// (handlers_bulk.go:64) — therefore proves the router did not gate the route,
// while leaving the per-element decision to the bulk handler's own tests.
func TestRouteScopes_PerElementRoutesHaveNoRouteGate(t *testing.T) {
	t.Parallel()

	fixture := newScopeProbeFixture(t)
	rows := expectedRouteScopes()

	// The foil's premise, asserted rather than assumed: the moment some route
	// requires ScopeClusterRead, this test silently stops proving anything.
	for _, row := range rows {
		require.NotEqual(t, internalauth.ScopeClusterRead, row.scope,
			"%s %s now requires ScopeClusterRead, which the per-element probe uses as a foil "+
				"precisely because no route accepts it — pick another unused scope",
			row.method, row.pattern)
	}

	probed := 0

	for _, row := range rows {
		if row.kind != kindPerElement {
			continue
		}

		probed++

		status := probe(t, probeRequest{
			handler: fixture.handler,
			method:  row.probeMethod(),
			pattern: row.pattern,
			token:   fixture.tokens[internalauth.ScopeClusterRead],
			body:    emptyBatchBody,
		})

		assert.NotEqual(t, http.StatusForbidden, status,
			"%s %s is declared kindPerElement but a route-level gate refused a token carrying only %s",
			row.method, row.pattern, internalauth.ScopeClusterRead)
	}

	require.NotZero(t, probed,
		"no kindPerElement row left to probe — the per-element contract went unverified")
}
