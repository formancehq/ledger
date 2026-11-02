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
// Canonical Ledger scopes use the app namespace followed by ActionResource,
// matching the Membership scope style (for example, ledger:ReadCluster).
var (
	ScopeLedgersRead       Scope = "ledger:ReadLedger"
	ScopeLedgersWrite      Scope = "ledger:WriteLedger"
	ScopeTransactionsRead  Scope = "ledger:ReadTransaction"
	ScopeTransactionsWrite Scope = "ledger:WriteTransaction"
	ScopeAccountsRead      Scope = "ledger:ReadAccount"
	ScopeMetadataWrite     Scope = "ledger:WriteMetadata"
	ScopeAuditRead         Scope = "ledger:ReadAudit"
	ScopeAuditWrite        Scope = "ledger:WriteAudit"
	ScopeOpsRead           Scope = "ledger:ReadOps"
	ScopeOpsWrite          Scope = "ledger:WriteOps"
	ScopeQueriesRead       Scope = "ledger:ReadQuery"
	ScopeQueriesWrite      Scope = "ledger:WriteQuery"
	ScopeClusterRead       Scope = "ledger:ReadCluster"
	ScopeClusterWrite      Scope = "ledger:WriteCluster"
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
// the list of granular scopes with a matching action prefix. Returns (nil, false)
// when the input is not a recognized wildcard.
func ExpandWildcardScope(s string) ([]Scope, bool) {
	switch s {
	case WildcardRead:
		return scopesWithAction("Read"), true
	case WildcardWrite:
		return scopesWithAction("Write"), true
	default:
		return nil, false
	}
}

// scopesWithAction returns every granular scope with the given action after the
// namespace separator (e.g. "Read", "Write"). Order is not stable.
func scopesWithAction(action string) []Scope {
	out := make([]Scope, 0, len(AllGranularScopes))
	for s := range AllGranularScopes {
		_, name, ok := strings.Cut(string(s), ":")
		if ok && strings.HasPrefix(name, action) {
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
