package auth

// Scope represents a granular authorization scope for the ledger service.
type Scope string

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

// HasScope checks whether the effective scope set contains all required scopes.
func HasScope(effective map[Scope]struct{}, required ...Scope) bool {
	for _, s := range required {
		if _, ok := effective[s]; !ok {
			return false
		}
	}

	return true
}
