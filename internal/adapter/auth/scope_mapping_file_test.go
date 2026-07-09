package auth

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseScopeMappingJSON_Valid(t *testing.T) {
	t.Parallel()

	data := []byte(`{
		"ledger:read": ["ledger:LedgerRead", "ledger:TransactionRead", "ledger:AccountRead"],
		"ledger:write": ["ledger:LedgerWrite", "ledger:TransactionWrite", "ledger:MetadataWrite"]
	}`)

	mapping, err := ParseScopeMappingJSON(data)
	require.NoError(t, err)
	assert.Len(t, mapping, 2)
	assert.Contains(t, mapping["ledger:read"], ScopeLedgersRead)
	assert.Contains(t, mapping["ledger:read"], ScopeTransactionsRead)
	assert.Contains(t, mapping["ledger:read"], ScopeAccountsRead)
	assert.Contains(t, mapping["ledger:write"], ScopeLedgersWrite)
	assert.Contains(t, mapping["ledger:write"], ScopeTransactionsWrite)
	assert.Contains(t, mapping["ledger:write"], ScopeMetadataWrite)
}

func TestParseScopeMappingJSON_UnknownScope(t *testing.T) {
	t.Parallel()

	data := []byte(`{
		"ledger:read": ["ledger:LedgerRead", "nonexistent:scope"]
	}`)

	_, err := ParseScopeMappingJSON(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown granular scope")
	assert.Contains(t, err.Error(), "nonexistent:scope")
}

func TestParseScopeMappingJSON_InvalidJSON(t *testing.T) {
	t.Parallel()

	_, err := ParseScopeMappingJSON([]byte(`{invalid`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing scope mapping JSON")
}

func TestParseScopeMappingJSON_Empty(t *testing.T) {
	t.Parallel()

	mapping, err := ParseScopeMappingJSON([]byte(`{}`))
	require.NoError(t, err)
	assert.Empty(t, mapping)
}

func TestLoadScopeMappingFromFile(t *testing.T) {
	t.Parallel()

	content := `{
		"custom:read": ["ledger:LedgerRead", "ledger:TransactionRead"],
		"custom:write": ["ledger:LedgerWrite"]
	}`

	dir := t.TempDir()
	path := filepath.Join(dir, "scope-mapping.json")
	require.NoError(t, os.WriteFile(path, []byte(content), 0o644))

	mapping, err := LoadScopeMappingFromFile(path)
	require.NoError(t, err)
	assert.Len(t, mapping, 2)
	assert.Contains(t, mapping["custom:read"], ScopeLedgersRead)
}

func TestLoadScopeMappingFromFile_NotFound(t *testing.T) {
	t.Parallel()

	_, err := LoadScopeMappingFromFile("/nonexistent/path.json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading scope mapping file")
}

func TestParseScopeMappingJSON_AllScopes(t *testing.T) {
	t.Parallel()

	// Verify all 14 granular scopes can be used in a mapping
	data := []byte(`{
		"all:read": ["ledger:LedgerRead", "ledger:TransactionRead", "ledger:AccountRead", "ledger:AuditRead", "ledger:OpsRead", "ledger:QueryRead", "ledger:ClusterRead"],
		"all:write": ["ledger:LedgerWrite", "ledger:TransactionWrite", "ledger:MetadataWrite", "ledger:AuditWrite", "ledger:OpsWrite", "ledger:QueryWrite", "ledger:ClusterWrite"]
	}`)

	mapping, err := ParseScopeMappingJSON(data)
	require.NoError(t, err)
	assert.Len(t, mapping["all:read"], 7)
	assert.Len(t, mapping["all:write"], 7)
}

func TestParseScopeMappingJSON_WildcardRead(t *testing.T) {
	t.Parallel()

	data := []byte(`{"anonymous": ["*:read"]}`)

	mapping, err := ParseScopeMappingJSON(data)
	require.NoError(t, err)

	anon := mapping.AnonymousScopes()
	assert.Contains(t, anon, ScopeLedgersRead)
	assert.Contains(t, anon, ScopeTransactionsRead)
	assert.Contains(t, anon, ScopeAccountsRead)
	assert.Contains(t, anon, ScopeAuditRead)
	assert.Contains(t, anon, ScopeOpsRead)
	assert.Contains(t, anon, ScopeQueriesRead)
	assert.Contains(t, anon, ScopeClusterRead)
	assert.NotContains(t, anon, ScopeTransactionsWrite)
}

func TestParseScopeMappingJSON_WildcardMixedWithExplicit(t *testing.T) {
	t.Parallel()

	data := []byte(`{
		"anonymous": ["*:read", "ledger:MetadataWrite"]
	}`)

	mapping, err := ParseScopeMappingJSON(data)
	require.NoError(t, err)

	anon := mapping.AnonymousScopes()
	assert.Contains(t, anon, ScopeLedgersRead)
	assert.Contains(t, anon, ScopeMetadataWrite)
	assert.NotContains(t, anon, ScopeTransactionsWrite)
}

func TestParseScopeMappingJSON_UnknownWildcard(t *testing.T) {
	t.Parallel()

	// "*:admin" is not a recognized wildcard and is not a granular scope either.
	data := []byte(`{"anonymous": ["*:admin"]}`)

	_, err := ParseScopeMappingJSON(data)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown granular scope")
}
