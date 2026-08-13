package http

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
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

// probeMethod is the method to send for a row: anyMethod rows share one handler
// and one middleware chain across all 9 methods, so one representative method
// asserts everything the other 8 would.
func (row routeScope) probeMethod() string {
	if row.anyMethod {
		return http.MethodGet
	}

	return row.method
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
		// here; the asymmetry is tracked in EN-1777.
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
//
// Do NOT reuse this outside the route-scope probes. Because Errorf is a no-op
// and nothing calls Finish, unmet expectations and calls with the wrong
// arguments are never reported: a handler test built on it would pass while
// asserting nothing. Ordinary handler tests take a gomock controller bound to
// their own *testing.T (see newTestServer).
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

// testTokenKeyID is the key ID carried by the key pair and the tokens below. Any
// value works: no assertion reads it back, it only has to match between the JWKS
// entry and the token header so the static key set resolves the key.
const testTokenKeyID = "test-key-id"

// testKeyPair generates an RSA key pair and returns the private key together with
// a static JWKS key set that verifies tokens signed by it.
//
// internal/adapter/auth has an equivalent helper in its own test files, and this
// is deliberately a package-local copy rather than a shared one: those tests are
// in-package (package auth), so nothing outside the auth package can import them,
// and the only way to share would be a package that exists solely for tests. A
// ~40-line fixture over an API that does not churn is the cheaper duplication.
func testKeyPair(t *testing.T) (*rsa.PrivateKey, oidc.KeySet) {
	t.Helper()

	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)

	jwk := jose.JSONWebKey{
		Key:       &privKey.PublicKey,
		KeyID:     testTokenKeyID,
		Algorithm: string(jose.RS256),
		Use:       "sig",
	}

	return privKey, oidc.NewStaticKeySet(jwk)
}

// testClaims builds access-token claims carrying the given token scopes, issued
// by testAuthIssuer and valid for one hour.
func testClaims(scopes ...string) *oidc.AccessTokenClaims {
	now := time.Now()

	claims := &oidc.AccessTokenClaims{}
	claims.Issuer = testAuthIssuer
	claims.Subject = "test-user"
	claims.IssuedAt = oidc.Time(now.Unix())
	claims.Expiration = oidc.Time(now.Add(1 * time.Hour).Unix())
	claims.Scopes = oidc.SpaceDelimitedArray(scopes)

	return claims
}

// signToken serialises claims as an RS256 compact JWS signed by key.
func signToken(t *testing.T, key *rsa.PrivateKey, claims *oidc.AccessTokenClaims) string {
	t.Helper()

	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       &jose.JSONWebKey{Key: key, KeyID: testTokenKeyID},
	}, nil)
	require.NoError(t, err)

	payload, err := json.Marshal(claims)
	require.NoError(t, err)

	jws, err := signer.Sign(payload)
	require.NoError(t, err)

	token, err := jws.CompactSerialize()
	require.NoError(t, err)

	return token
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

	_, keySet := testKeyPair(t)
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
// well-formed, so the reconciliation and the probes actually cover every row: no
// endpoint declared twice, and a scope declared on exactly the rows that have a
// gate — one that really exists.
//
// It deliberately does NOT pin row counts. The frozen 74/68/5/1 counts this
// replaced trained the habit of bumping a number until green, and their failure
// message ("expected 74, got 75") said nothing about what to do. Removing them
// gave up one thing knowingly: detection of a partial truncation that deletes
// rows *and* their routes together. That case is covered where it matters
// instead, by the require.NotZero guard in each of the three probe tests, which
// fails with a sentence rather than a number. Do not reintroduce the counts.
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

	// A cross-check on expandTable rather than an independent fact: with the
	// duplicates it just rejected, this identity holds by construction. It is kept
	// because it would catch the helper dropping or double-counting a fan-out.
	require.Len(t, expanded, singleMethodRows+len(anyMethodMethods)*anyMethodRows,
		"expanding %d single-method rows and %d anyMethod rows must yield one entry per endpoint",
		singleMethodRows, anyMethodRows)

	// A scope is declared on exactly the guarded rows, and it is a real granular
	// scope. All three directions are load-bearing:
	//
	//   - a kindGuarded row with no scope, or one that is not in
	//     AllGranularScopes, mints no token (scopeProbeFixture keys its tokens on
	//     that set), so the probe sends no Authorization header and the failure
	//     reads "must admit its declared scope X but answered 401" — blaming the
	//     router for a malformed row. This check is also what makes the token
	//     lookups in the probe tests safe without a per-read ok check.
	//   - a scope on a kindPublic or kindPerElement row is read by nothing at all,
	//     in either test, so it would drift unnoticed forever.
	//   - a row whose kind is none of the three is skipped by every probe test —
	//     each loop is a `row.kind != kindX { continue }` — while still satisfying
	//     route reconciliation, which compares (method, pattern) only. That route's
	//     authorization would go entirely unverified, and the per-loop
	//     require.NotZero guards stay green off the other rows. So a fourth kind
	//     fails here until its own probe loop exists.
	var malformed []string

	for _, row := range rows {
		_, granular := internalauth.AllGranularScopes[row.scope]

		switch {
		case row.kind != kindGuarded && row.kind != kindPublic && row.kind != kindPerElement:
			malformed = append(malformed, fmt.Sprintf(
				"%s %s declares routeKind %d, which no probe test covers — add its probe loop",
				row.method, row.pattern, row.kind))
		case row.kind == kindGuarded && row.scope == "":
			malformed = append(malformed, fmt.Sprintf(
				"%s %s is kindGuarded but declares no scope", row.method, row.pattern))
		case row.kind == kindGuarded && !granular:
			malformed = append(malformed, fmt.Sprintf(
				"%s %s is kindGuarded but declares %q, which is not in internalauth.AllGranularScopes",
				row.method, row.pattern, row.scope))
		case row.kind != kindGuarded && row.scope != "":
			malformed = append(malformed, fmt.Sprintf(
				"%s %s is not kindGuarded but declares scope %s", row.method, row.pattern, row.scope))
		}
	}

	slices.Sort(malformed)
	require.Empty(t, malformed,
		"a row must carry one of the three known kinds, and declare a scope if and only if it is "+
			"kindGuarded: %s",
		strings.Join(malformed, "; "))
}

// pathParamValues instantiates chi patterns with concrete, plain-ASCII values.
//
// Two constraints on the values. ASCII matters: utf8PathParamValidator runs
// before RequireScope (handler.go:102), so a value it rejects would
// short-circuit with 400 and hide the scope decision under test. And a value
// must not collide with a sibling static segment of any route it substitutes
// into — "{canonicalId}" = "status" would make /v3/_/indexes/{canonicalId}
// resolve to the static /v3/_/indexes/status instead, silently probing a
// different route. The RoutePattern assertion in probe catches that class.
//
// Iterating this map in random order is safe: every key is brace-delimited, and
// no placeholder is a substring of another, so no substitution can create or
// destroy a match for a later one.
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

	// reachesEndpoint says the request is expected to get past any scope gate.
	// Only then has chi recorded the full matched pattern, so only then can probe
	// verify it — see the note there.
	reachesEndpoint bool
}

// probe drives one request through the real middleware chain and returns the
// status, asserting on the way that the request reached the route it claims to be
// probing whenever that is observable.
//
// End-to-end requests are the only sound oracle for the scope a route requires.
// The middleware slice chi.Walk exposes is not one: `With(mw).Route(...)`
// attaches mw to the subrouter's handler rather than to Middlewares(), so
// /v3/_/chapters shows no RequireScope yet answers 401 tokenless — and all ten
// RequireScope closures share a single code pointer anyway.
//
// That same `Route` mechanic is why the pattern check is conditional. A gate
// mounted with `With(mw).Route(prefix, ...)` runs on the SUB-mux's handler, i.e.
// before the sub-mux routes the remainder of the path, so a request it refuses
// records only the prefix: the pprof and /v3/_ subtrees report "/debug/pprof/*"
// and "/v3/_/*" on a 403. A gate mounted with `With(mw).Group(...)` shares the
// parent's route tree, so there the full pattern is recorded either way
// (/v3/{ledgerName}/stats reports in full on a 403). The pattern is therefore
// complete exactly when the request reaches the endpoint, which is what
// reachesEndpoint declares.
//
// Skipping the check on a refused probe costs nothing: for a given row the
// concrete target is byte-identical across its positive and negative probes, so
// the positive one already proved that target matches the declared route.
func probe(t *testing.T, in probeRequest) int {
	t.Helper()

	req := httptest.NewRequest(in.method, concreteTarget(t, in.pattern), strings.NewReader(in.body))
	req.Header.Set("Content-Type", "application/json")

	if in.token != "" {
		req.Header.Set("Authorization", "Bearer "+in.token)
	}

	// chi.Mux.ServeHTTP reuses a route context already present on the request
	// instead of taking one from its pool (chi mux.go:70-75), so after the request
	// this context reports which route the probe actually reached.
	rctx := chi.NewRouteContext()
	ctx := context.WithValue(cancelledProbeContext(), chi.RouteCtxKey, rctx)

	w := httptest.NewRecorder()
	in.handler.ServeHTTP(w, req.WithContext(ctx))

	if in.reachesEndpoint {
		// require, not assert: a probe on the wrong route is a broken test rather
		// than a finding about the router, and every status derived from it is
		// meaningless. This is what keeps the positive assertion honest — see the
		// note on TestRouteScopes_GuardedRoutesRequireExactlyOneScope.
		require.Equal(t, chiRoutePattern(in.pattern), rctx.RoutePattern(),
			"probing %s %s reached a different route — the concrete target built from pathParamValues "+
				"matched something else, so this probe asserts nothing about the declared route",
			in.method, in.pattern)
	}

	return w.Code
}

// chiRoutePattern normalizes a declared pattern the way chi reports a matched
// one, so the two can be compared.
//
// Context.RoutePattern joins the patterns of every router the request passed
// through and then drops a trailing slash unless the result is exactly "/"
// (chi context.go:127-133). The two routes registered as "/" inside a subrouter
// — "/v3/" and "/debug/pprof/" as chi.Walk reports them — therefore come back
// without it. Nothing else in the declared table is affected.
func chiRoutePattern(pattern string) string {
	if pattern == "/" {
		return pattern
	}

	return strings.TrimSuffix(pattern, "/")
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

// sortedGranularScopes returns every granular scope in a deterministic order.
// internalauth.AllGranularScopes is a map, so probing it directly would name a
// different offending scope on each run and read as flakiness.
func sortedGranularScopes() []internalauth.Scope {
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
// per-route loop: testKeyPair generates a 2048-bit key, which would otherwise
// dominate the runtime of a route × scope sweep.
//
// tokens holds an entry for every granular scope, and every kindGuarded row is
// asserted to declare one of those by
// TestRouteScopes_TableComposition, so callers can index it with a row's scope
// without checking the result.
type scopeProbeFixture struct {
	handler http.Handler
	tokens  map[internalauth.Scope]string
}

func newScopeProbeFixture(t *testing.T) scopeProbeFixture {
	t.Helper()

	key, keySet := testKeyPair(t)

	tokens := make(map[internalauth.Scope]string, len(internalauth.AllGranularScopes))

	for _, scope := range sortedGranularScopes() {
		// A granular scope is not a key of DefaultMapping, so it reaches the
		// gate through ExpandScopes' identity pass-through: exactly one granular
		// scope per token, never an aggregate that would expand to several.
		tokens[scope] = signToken(t, key, testClaims(string(scope)))
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
//
// The two halves are load-bearing together, and the positive one is the weaker.
// "Neither 401 nor 403" also holds for a 404, so on its own it would pass for a
// route the request never reached. Two things rule that out: probe asserts the
// matched route pattern, and each of the 13 paired negatives demands an exact
// 403, which is unreachable unless chi matched the route and ran its
// RequireScope. Do not weaken the negatives to "not 200" — that would delete
// half the argument.
func TestRouteScopes_GuardedRoutesRequireExactlyOneScope(t *testing.T) {
	t.Parallel()

	fixture := newScopeProbeFixture(t)
	scopes := sortedGranularScopes()
	probed := 0

	// Unlike the reconciliation loops, these do not aggregate-and-sort their
	// findings: they walk a deterministically ordered slice of rows and scopes, so
	// the failures come out in the same order on every run already.
	for _, row := range expectedRouteScopes() {
		if row.kind != kindGuarded {
			continue
		}

		probed++

		method := row.probeMethod()

		admitted := probe(t, probeRequest{
			handler:         fixture.handler,
			method:          method,
			pattern:         row.pattern,
			token:           fixture.tokens[row.scope],
			body:            probeBody,
			reachesEndpoint: true,
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

			// reachesEndpoint stays false: the gate is expected to refuse this, and
			// under a Route-mounted gate chi has not recorded the full pattern yet.
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

	require.NotZero(t, probed,
		"no kindGuarded row left to probe — the scope contract went unverified")
}

// TestRouteScopes_PublicRoutesNeedNoToken checks the kindPublic rows are
// reachable with no Authorization header at all.
//
// It builds a bare handler rather than a scopeProbeFixture: no token is ever
// sent here, and minting a key pair plus 14 unused tokens would only obscure
// that.
func TestRouteScopes_PublicRoutesNeedNoToken(t *testing.T) {
	t.Parallel()

	_, keySet := testKeyPair(t)
	handler := newScopedHandler(t, keySet)

	probed := 0

	for _, row := range expectedRouteScopes() {
		if row.kind != kindPublic {
			continue
		}

		probed++

		method := row.probeMethod()

		status := probe(t, probeRequest{
			handler:         handler,
			method:          method,
			pattern:         row.pattern,
			body:            probeBody,
			reachesEndpoint: true,
		})

		assert.NotEqual(t, http.StatusUnauthorized, status,
			"%s %s is declared kindPublic but answered 401 without a token", method, row.pattern)
		assert.NotEqual(t, http.StatusForbidden, status,
			"%s %s is declared kindPublic but answered 403 without a token", method, row.pattern)
	}

	require.NotZero(t, probed, "no kindPublic row left to probe — the public contract went unverified")
}

// TestRouteScopes_PerElementRoutesHaveNoRouteGate checks the kindPerElement rows
// carry no route-level gate.
//
// The probe sends no token at all, which is what makes it exhaustive: a
// tokenless request reaches RequireScope with the anonymous scope set, so *any*
// route-level gate answers 401 (http_middleware.go:110-125) whatever scope it
// demands. A foil token could only rule out the one scope it carries — a gate
// requiring exactly that scope would admit it, leave the row kindPerElement, and
// keep this test green over a route that just acquired a route-level gate.
//
// Reaching the handler with an empty batch — which has no element to authorize
// (handlers_bulk.go:66-91) — therefore proves the router did not gate the route,
// while leaving the per-element decision to the bulk handler's own tests.
func TestRouteScopes_PerElementRoutesHaveNoRouteGate(t *testing.T) {
	t.Parallel()

	// The premise the tokenless oracle rests on, asserted rather than assumed:
	// were the mapping to grant scopes anonymously, a route-level gate could
	// admit this probe and the test would silently stop proving anything. It is
	// read off the mapping newScopedHandler builds the router with, not off the
	// declared table this test verifies, so it stays a real check.
	require.Empty(t, internalauth.DefaultMapping("ledger").AnonymousScopes(),
		"the default mapping now grants scopes anonymously, so a tokenless request no longer "+
			"proves the absence of a route-level gate — probe with an explicitly empty scope mapping")

	_, keySet := testKeyPair(t)
	handler := newScopedHandler(t, keySet)

	probed := 0

	for _, row := range expectedRouteScopes() {
		if row.kind != kindPerElement {
			continue
		}

		probed++

		method := row.probeMethod()

		status := probe(t, probeRequest{
			handler:         handler,
			method:          method,
			pattern:         row.pattern,
			body:            emptyBatchBody,
			reachesEndpoint: true,
		})

		assert.NotEqual(t, http.StatusUnauthorized, status,
			"%s %s is declared kindPerElement but a route-level gate refused a tokenless request",
			method, row.pattern)
		// Unreachable through either gate tokenless — RequireScope answers 401
		// when no token was presented, and the bulk handler's per-element loop
		// has no element to refuse — so a 403 here is an unmodelled gate.
		assert.NotEqual(t, http.StatusForbidden, status,
			"%s %s is declared kindPerElement but answered 403 to a tokenless request",
			method, row.pattern)
	}

	require.NotZero(t, probed,
		"no kindPerElement row left to probe — the per-element contract went unverified")
}
