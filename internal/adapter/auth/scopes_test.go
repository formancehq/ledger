package auth

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultMapping_DefaultService(t *testing.T) {
	t.Parallel()

	m := DefaultMapping("")
	assert.Contains(t, m, "ledger:read")
	assert.Contains(t, m, "ledger:write")
	assert.Contains(t, m, "ledger:admin")
	assert.Contains(t, m["ledger:read"], ScopeLedgersRead)
	assert.Contains(t, m["ledger:read"], ScopeTransactionsRead)
	assert.Contains(t, m["ledger:read"], ScopeAccountsRead)
	assert.Contains(t, m["ledger:write"], ScopeLedgersWrite)
	assert.Contains(t, m["ledger:write"], ScopeTransactionsWrite)
	assert.Contains(t, m["ledger:write"], ScopeMetadataWrite)
	assert.Contains(t, m["ledger:admin"], ScopeClusterRead)
	assert.Contains(t, m["ledger:admin"], ScopeClusterWrite)
}

func TestDefaultMapping_CustomService(t *testing.T) {
	t.Parallel()

	m := DefaultMapping("myapp")
	assert.Contains(t, m, "myapp:read")
	assert.Contains(t, m, "myapp:write")
	assert.Contains(t, m, "myapp:admin")
	assert.NotContains(t, m, "ledger:read")
}

func TestExpandScopes_VirtualToGranular(t *testing.T) {
	t.Parallel()

	m := DefaultMapping("ledger")
	effective := m.ExpandScopes([]string{"ledger:read", "ledger:write"})

	// Should contain all read granular scopes
	assert.Contains(t, effective, ScopeLedgersRead)
	assert.Contains(t, effective, ScopeTransactionsRead)
	assert.Contains(t, effective, ScopeAccountsRead)
	assert.Contains(t, effective, ScopeAuditRead)
	assert.Contains(t, effective, ScopeOpsRead)
	assert.Contains(t, effective, ScopeQueriesRead)

	// Should contain all write granular scopes
	assert.Contains(t, effective, ScopeLedgersWrite)
	assert.Contains(t, effective, ScopeTransactionsWrite)
	assert.Contains(t, effective, ScopeMetadataWrite)
	assert.Contains(t, effective, ScopeAuditWrite)
	assert.Contains(t, effective, ScopeOpsWrite)
	assert.Contains(t, effective, ScopeQueriesWrite)

	// Should NOT contain cluster scopes (that's admin)
	assert.NotContains(t, effective, ScopeClusterRead)
	assert.NotContains(t, effective, ScopeClusterWrite)
}

func TestExpandScopes_IdentityPassThrough(t *testing.T) {
	t.Parallel()

	m := DefaultMapping("ledger")
	// Token has a granular scope directly
	effective := m.ExpandScopes([]string{"transactions:read"})

	assert.Contains(t, effective, ScopeTransactionsRead)
	// Should NOT expand to other scopes
	assert.NotContains(t, effective, ScopeLedgersRead)
}

func TestExpandScopes_MixedVirtualAndGranular(t *testing.T) {
	t.Parallel()

	m := DefaultMapping("ledger")
	effective := m.ExpandScopes([]string{"ledger:admin", "transactions:read"})

	// From virtual scope
	assert.Contains(t, effective, ScopeClusterRead)
	assert.Contains(t, effective, ScopeClusterWrite)
	// From identity pass-through
	assert.Contains(t, effective, ScopeTransactionsRead)
	// Should NOT have other read scopes
	assert.NotContains(t, effective, ScopeLedgersRead)
}

func TestExpandScopes_UnknownScope(t *testing.T) {
	t.Parallel()

	m := DefaultMapping("ledger")
	effective := m.ExpandScopes([]string{"unknown:scope"})

	assert.Empty(t, effective)
}

func TestHasScope(t *testing.T) {
	t.Parallel()

	effective := map[Scope]struct{}{
		ScopeLedgersRead:      {},
		ScopeTransactionsRead: {},
	}

	assert.True(t, HasScope(effective, ScopeLedgersRead))
	assert.True(t, HasScope(effective, ScopeTransactionsRead))
	assert.True(t, HasScope(effective, ScopeLedgersRead, ScopeTransactionsRead))
	assert.False(t, HasScope(effective, ScopeLedgersWrite))
	assert.False(t, HasScope(effective, ScopeLedgersRead, ScopeLedgersWrite))
}

func TestHasScope_Empty(t *testing.T) {
	t.Parallel()

	effective := map[Scope]struct{}{}
	// No required scopes = always true
	assert.True(t, HasScope(effective))
}

func TestExpandWildcardScope_Read(t *testing.T) {
	t.Parallel()

	scopes, ok := ExpandWildcardScope(WildcardRead)
	assert.True(t, ok)

	got := make(map[Scope]struct{}, len(scopes))
	for _, s := range scopes {
		got[s] = struct{}{}
	}

	assert.Contains(t, got, ScopeLedgersRead)
	assert.Contains(t, got, ScopeTransactionsRead)
	assert.Contains(t, got, ScopeAccountsRead)
	assert.Contains(t, got, ScopeAuditRead)
	assert.Contains(t, got, ScopeOpsRead)
	assert.Contains(t, got, ScopeQueriesRead)
	assert.Contains(t, got, ScopeClusterRead)
	assert.NotContains(t, got, ScopeLedgersWrite)
	assert.NotContains(t, got, ScopeMetadataWrite)
}

func TestExpandWildcardScope_Write(t *testing.T) {
	t.Parallel()

	scopes, ok := ExpandWildcardScope(WildcardWrite)
	assert.True(t, ok)

	got := make(map[Scope]struct{}, len(scopes))
	for _, s := range scopes {
		got[s] = struct{}{}
	}

	assert.Contains(t, got, ScopeLedgersWrite)
	assert.Contains(t, got, ScopeTransactionsWrite)
	assert.Contains(t, got, ScopeMetadataWrite)
	assert.Contains(t, got, ScopeAuditWrite)
	assert.Contains(t, got, ScopeOpsWrite)
	assert.Contains(t, got, ScopeQueriesWrite)
	assert.Contains(t, got, ScopeClusterWrite)
	assert.NotContains(t, got, ScopeLedgersRead)
}

func TestExpandWildcardScope_Unknown(t *testing.T) {
	t.Parallel()

	_, ok := ExpandWildcardScope("*:admin")
	assert.False(t, ok)

	_, ok = ExpandWildcardScope("ledgers:read")
	assert.False(t, ok)
}

func TestAnonymousScopes_NotConfigured(t *testing.T) {
	t.Parallel()

	m := DefaultMapping("ledger")
	assert.Nil(t, m.AnonymousScopes(), "default mapping must not grant anonymous access")
}

func TestAnonymousScopes_Configured(t *testing.T) {
	t.Parallel()

	m := DefaultMapping("ledger")
	m[ScopeMappingAnonymousKey] = []Scope{ScopeLedgersRead, ScopeAccountsRead}

	anon := m.AnonymousScopes()
	assert.Contains(t, anon, ScopeLedgersRead)
	assert.Contains(t, anon, ScopeAccountsRead)
	assert.NotContains(t, anon, ScopeTransactionsWrite)
}
