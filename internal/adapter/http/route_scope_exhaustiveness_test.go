package http

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/go-chi/chi/v5"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/adapter/auth/authtest"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
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
// router: 74 rows, expanding to the 122 entries chi.Walk reports.
//
// Adding a route to handler.go without adding a row here fails
// TestRouteScopes_Exhaustive. That is the point: the scope a route requires is
// an authorization decision, and it must be made explicitly rather than
// inherited from whichever Group block happens to enclose the line.
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

		// --- pprof (handler.go:77-89), all behind requireOpsRead ---
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

		// --- Ledgers read (handler.go:104-117) ---
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

		// --- Transactions read (handler.go:120-123) ---
		{http.MethodGet, "/v3/{ledgerName}/transactions", kindGuarded, internalauth.ScopeTransactionsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/transactions/{transactionId}", kindGuarded, internalauth.ScopeTransactionsRead, false},

		// --- Audit read (handler.go:132-135) ---
		{http.MethodGet, "/v3/_/audit-entries", kindGuarded, internalauth.ScopeAuditRead, false},
		{http.MethodGet, "/v3/_/audit-entries/{sequence}", kindGuarded, internalauth.ScopeAuditRead, false},

		// --- Ops read, bucket-wide /_ subtree (handler.go:147-157) ---
		{http.MethodGet, "/v3/_/logs/{sequence}", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/chapters", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/chapter-schedule", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/events-sinks", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/signing-keys", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/indexes", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/indexes/status", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/indexes/{canonicalId}", kindGuarded, internalauth.ScopeOpsRead, false},
		{http.MethodGet, "/v3/_/indexes/{canonicalId}/status", kindGuarded, internalauth.ScopeOpsRead, false},

		// --- Accounts read (handler.go:160-169) ---
		{http.MethodGet, "/v3/{ledgerName}/accounts", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/accounts/{address}", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/volumes", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/metadata-schema", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/analyze-accounts", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/analyze-transactions", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/account-types", kindGuarded, internalauth.ScopeAccountsRead, false},
		{http.MethodGet, "/v3/{ledgerName}/account-types/{typeName}", kindGuarded, internalauth.ScopeAccountsRead, false},

		// --- Ledgers write (handler.go:172-179) ---
		{http.MethodPost, "/v3/{ledgerName}", kindGuarded, internalauth.ScopeLedgersWrite, false},
		{http.MethodDelete, "/v3/{ledgerName}", kindGuarded, internalauth.ScopeLedgersWrite, false},
		{http.MethodPost, "/v3/{ledgerName}/promote", kindGuarded, internalauth.ScopeLedgersWrite, false},
		{http.MethodPut, "/v3/{ledgerName}/numscripts/{name}", kindGuarded, internalauth.ScopeLedgersWrite, false},
		{http.MethodPost, "/v3/{ledgerName}/indexes", kindGuarded, internalauth.ScopeLedgersWrite, false},
		{http.MethodDelete, "/v3/{ledgerName}/indexes/{canonicalId}", kindGuarded, internalauth.ScopeLedgersWrite, false},

		// --- Transactions write (handler.go:182-185) ---
		{http.MethodPost, "/v3/{ledgerName}/transactions", kindGuarded, internalauth.ScopeTransactionsWrite, false},
		{http.MethodPost, "/v3/{ledgerName}/transactions/{transactionId}/revert", kindGuarded, internalauth.ScopeTransactionsWrite, false},

		// --- Metadata write (handler.go:188-200) ---
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

		// --- Prepared queries read (handler.go:206-209) ---
		{http.MethodGet, "/v3/{ledgerName}/prepared-queries", kindGuarded, internalauth.ScopeQueriesRead, false},
		{http.MethodPost, "/v3/{ledgerName}/prepared-queries/{queryName}/execute", kindGuarded, internalauth.ScopeQueriesRead, false},

		// --- Prepared queries write (handler.go:212-216) ---
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

// inertBackend returns a Backend that is deliberately not stubbed.
//
// The probes below assert only the authorization outcome, so what a handler
// does after the gate is irrelevant: an unstubbed call unwinds and jsonRecoverer
// turns it into a 500, which is neither 401 nor 403. The alternative — stubbing
// all ~30 Controller methods with AnyTimes — would make this test break every
// time Controller gains a method, which is the opposite of what a stable
// authorization guard should do.
func inertBackend(t *testing.T) *MockBackend {
	t.Helper()

	return NewMockBackend(gomock.NewController(ignoringReporter{}))
}

// ignoringReporter turns gomock's unexpected-call report into a panic instead of
// failing the test, so it surfaces as the 500 described on inertBackend.
type ignoringReporter struct{}

func (ignoringReporter) Errorf(_ string, _ ...any) {}

func (ignoringReporter) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// newScopedHandler builds the real router with authentication enabled.
func newScopedHandler(t *testing.T, keySet oidc.KeySet) http.Handler {
	t.Helper()

	return NewHandler(logging.Testing(), inertBackend(t), internalauth.AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		Issuer:       testAuthIssuer,
		Service:      "ledger",
		ScopeMapping: internalauth.DefaultMapping("ledger"),
	}, version.Info{})
}

const testAuthIssuer = "https://issuer.test"

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

// expandTable expands anyMethod rows into one entry per HTTP method.
func expandTable(rows []routeScope) map[walkKey]routeScope {
	expanded := map[walkKey]routeScope{}

	for _, row := range rows {
		if !row.anyMethod {
			expanded[walkKey{row.method, row.pattern}] = row

			continue
		}

		for _, method := range anyMethodMethods {
			expanded[walkKey{method, row.pattern}] = row
		}
	}

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
	expanded := expandTable(expectedRouteScopes())

	for key := range walked {
		_, ok := expanded[key]
		require.True(t, ok,
			"%s %s is registered by handler.go but has no row in expectedRouteScopes — "+
				"add one declaring the granular scope it requires (or kindPublic / "+
				"kindPerElement), so the authorization decision is explicit",
			key.method, key.pattern)
	}

	for key := range expanded {
		_, ok := walked[key]
		require.True(t, ok,
			"expectedRouteScopes declares %s %s which the router no longer registers — remove the row",
			key.method, key.pattern)
	}
}

// TestRouteScopes_TableComposition pins the shape of the declared table. A
// bulk edit that drops rows would still reconcile if it dropped routes too;
// this catches the table being truncated on its own.
func TestRouteScopes_TableComposition(t *testing.T) {
	t.Parallel()

	rows := expectedRouteScopes()

	byKind := map[routeKind]int{}
	for _, row := range rows {
		byKind[row.kind]++
	}

	require.Len(t, rows, 74, "declared table must hold one row per endpoint")
	require.Equal(t, 68, byKind[kindGuarded], "guarded routes")
	require.Equal(t, 5, byKind[kindPublic], "public routes")
	require.Equal(t, 1, byKind[kindPerElement], "per-element routes")
	require.Len(t, expandTable(rows), 122, "expanded table must match the chi.Walk entry count")
}
