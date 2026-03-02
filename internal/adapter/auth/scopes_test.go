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
