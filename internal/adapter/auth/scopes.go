package auth

import "strings"

// Scope represents a granular authorization scope for the ledger service.
type Scope string

// ScopeMappingAnonymousKey is the reserved virtual-scope key whose granular
// scopes are granted to any request that does not present a bearer token.
// When absent from the mapping (default), unauthenticated requests get no
// scopes — preserving the historical strict behavior.
const ScopeMappingAnonymousKey = "anonymous"

// Wildcard tokens accepted inside scope mapping value lists.
const (
	WildcardRead  = "*:read"
	WildcardWrite = "*:write"
)

// Granular scopes — 14 total, grouped by resource category.
var (
	ScopeLedgersRead       Scope = "ledgers:read"
	ScopeLedgersWrite      Scope = "ledgers:write"
	ScopeTransactionsRead  Scope = "transactions:read"
	ScopeTransactionsWrite Scope = "transactions:write"
	ScopeAccountsRead      Scope = "accounts:read"
	ScopeMetadataWrite     Scope = "metadata:write"
	ScopeAuditRead         Scope = "audit:read"
	ScopeAuditWrite        Scope = "audit:write"
	ScopeOpsRead           Scope = "ops:read"
	ScopeOpsWrite          Scope = "ops:write"
	ScopeQueriesRead       Scope = "queries:read"
	ScopeQueriesWrite      Scope = "queries:write"
	ScopeClusterRead       Scope = "cluster:read"
	ScopeClusterWrite      Scope = "cluster:write"
)

// AllGranularScopes is the complete set of valid granular scopes for validation.
var AllGranularScopes = map[Scope]struct{}{
	ScopeLedgersRead:       {},
	ScopeLedgersWrite:      {},
	ScopeTransactionsRead:  {},
	ScopeTransactionsWrite: {},
	ScopeAccountsRead:      {},
	ScopeMetadataWrite:     {},
	ScopeAuditRead:         {},
	ScopeAuditWrite:        {},
	ScopeOpsRead:           {},
	ScopeOpsWrite:          {},
	ScopeQueriesRead:       {},
	ScopeQueriesWrite:      {},
	ScopeClusterRead:       {},
	ScopeClusterWrite:      {},
}

// ScopeMapping maps virtual scopes (the ones in JWT tokens, e.g. "ledger:read")
// to granular scopes (the internal per-category scopes).
type ScopeMapping map[string][]Scope

// DefaultMapping builds the backward-compatible mapping using the given service prefix.
// Keys are "{service}:read", "{service}:write", "{service}:admin".
func DefaultMapping(service string) ScopeMapping {
	if service == "" {
		service = "ledger"
	}

	return ScopeMapping{
		service + ":read": {
			ScopeLedgersRead,
			ScopeTransactionsRead,
			ScopeAccountsRead,
			ScopeAuditRead,
			ScopeOpsRead,
			ScopeQueriesRead,
		},
		service + ":write": {
			ScopeLedgersWrite,
			ScopeTransactionsWrite,
			ScopeMetadataWrite,
			ScopeAuditWrite,
			ScopeOpsWrite,
			ScopeQueriesWrite,
		},
		service + ":admin": {
			ScopeClusterRead,
			ScopeClusterWrite,
		},
	}
}

// ExpandScopes expands a list of token scopes through the mapping, returning
// the effective set of granular scopes. Token scopes that are themselves valid
// granular scopes pass through directly (identity pass-through).
func (m ScopeMapping) ExpandScopes(tokenScopes []string) map[Scope]struct{} {
	result := make(map[Scope]struct{})

	for _, ts := range tokenScopes {
		// Try mapping expansion first
		if granularScopes, ok := m[ts]; ok {
			for _, gs := range granularScopes {
				result[gs] = struct{}{}
			}
		}
		// Identity pass-through: if the token scope is itself a valid granular scope
		if _, ok := AllGranularScopes[Scope(ts)]; ok {
			result[Scope(ts)] = struct{}{}
		}
	}

	return result
}

// AnonymousScopes returns the granular scopes granted to unauthenticated
// requests, derived by expanding the reserved "anonymous" virtual scope.
// Returns nil if the mapping has no "anonymous" key.
func (m ScopeMapping) AnonymousScopes() map[Scope]struct{} {
	if _, ok := m[ScopeMappingAnonymousKey]; !ok {
		return nil
	}

	return m.ExpandScopes([]string{ScopeMappingAnonymousKey})
}

// ExpandWildcardScope expands a wildcard token (e.g. "*:read", "*:write") into
// the list of granular scopes that share its suffix. Returns (nil, false) when
// the input is not a recognized wildcard.
func ExpandWildcardScope(s string) ([]Scope, bool) {
	switch s {
	case WildcardRead:
		return scopesWithSuffix(":read"), true
	case WildcardWrite:
		return scopesWithSuffix(":write"), true
	default:
		return nil, false
	}
}

// scopesWithSuffix returns every granular scope whose name ends with the given
// suffix (e.g. ":read", ":write"). Order is not stable.
func scopesWithSuffix(suffix string) []Scope {
	out := make([]Scope, 0, len(AllGranularScopes))
	for s := range AllGranularScopes {
		if strings.HasSuffix(string(s), suffix) {
			out = append(out, s)
		}
	}

	return out
}

// HasScope checks whether the effective scope set contains all required scopes.
func HasScope(effective map[Scope]struct{}, required ...Scope) bool {
	for _, s := range required {
		if _, ok := effective[s]; !ok {
			return false
		}
	}

	return true
}
