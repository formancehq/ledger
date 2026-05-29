package bootstrap

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestLoadScopeMapping_FromFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	mappingFile := filepath.Join(dir, "scope-mapping.json")

	mapping := map[string][]string{
		"custom:read": {"ledgers:read", "transactions:read"},
	}

	data, err := json.Marshal(mapping)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(mappingFile, data, 0o600))

	logger := logging.Testing()
	cfg := Config{
		AuthConfig: AuthFlagConfig{
			ScopeMappingFile: mappingFile,
		},
	}

	result, err := loadScopeMapping(cfg, logger)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result, 1)
	assert.Contains(t, result, "custom:read")
	assert.Equal(t, []internalauth.Scope{internalauth.ScopeLedgersRead, internalauth.ScopeTransactionsRead}, result["custom:read"])
}

func TestLoadScopeMapping_FromEnvJSON(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()

	rawJSON := `{"my-scope:write": ["ledgers:write", "metadata:write"]}`
	cfg := Config{
		AuthConfig: AuthFlagConfig{
			ScopeMappingJSON: rawJSON,
		},
	}

	result, err := loadScopeMapping(cfg, logger)
	require.NoError(t, err)
	require.NotNil(t, result)
	assert.Len(t, result, 1)
	assert.Contains(t, result, "my-scope:write")
	assert.Equal(t, []internalauth.Scope{internalauth.ScopeLedgersWrite, internalauth.ScopeMetadataWrite}, result["my-scope:write"])
}

func TestLoadScopeMapping_DefaultMapping(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()

	cfg := Config{
		AuthConfig: AuthFlagConfig{
			Service: "myservice",
		},
	}

	result, err := loadScopeMapping(cfg, logger)
	require.NoError(t, err)
	require.NotNil(t, result)

	// DefaultMapping uses service prefix
	assert.Contains(t, result, "myservice:read")
	assert.Contains(t, result, "myservice:write")
	assert.Contains(t, result, "myservice:admin")
}

func TestLoadScopeMapping_DefaultMappingEmptyService(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()

	cfg := Config{}

	result, err := loadScopeMapping(cfg, logger)
	require.NoError(t, err)
	require.NotNil(t, result)

	// DefaultMapping with empty service uses "ledger"
	assert.Contains(t, result, "ledger:read")
	assert.Contains(t, result, "ledger:write")
	assert.Contains(t, result, "ledger:admin")
}

func TestLoadScopeMapping_FileError(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	cfg := Config{
		AuthConfig: AuthFlagConfig{
			ScopeMappingFile: "/nonexistent/scope-mapping.json",
		},
	}

	_, err := loadScopeMapping(cfg, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "loading scope mapping file")
}

func TestLoadScopeMapping_InvalidJSON(t *testing.T) {
	t.Parallel()

	logger := logging.Testing()
	cfg := Config{
		AuthConfig: AuthFlagConfig{
			ScopeMappingJSON: `{invalid json}`,
		},
	}

	_, err := loadScopeMapping(cfg, logger)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "parsing AUTH_SCOPE_MAPPING env var")
}

func TestLoadScopeMapping_FilePrecedenceOverJSON(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	mappingFile := filepath.Join(dir, "scope-mapping.json")

	mapping := map[string][]string{
		"file:read": {"ledgers:read"},
	}

	data, err := json.Marshal(mapping)
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(mappingFile, data, 0o600))

	logger := logging.Testing()
	cfg := Config{
		AuthConfig: AuthFlagConfig{
			ScopeMappingFile: mappingFile,
			ScopeMappingJSON: `{"json:read": ["ledgers:read"]}`,
		},
	}

	result, err := loadScopeMapping(cfg, logger)
	require.NoError(t, err)

	// File takes precedence over JSON
	assert.Contains(t, result, "file:read")
	assert.NotContains(t, result, "json:read")
}

func TestPersistConfig_SaveAndLoad(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	cfg := &commonpb.PersistedConfig{
		NodeId:    42,
		ClusterId: "my-cluster",
	}

	err := persistConfig(store, cfg)
	require.NoError(t, err)

	loaded, err := LoadPersistedConfig(store)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, uint64(42), loaded.GetNodeId())
	assert.Equal(t, "my-cluster", loaded.GetClusterId())
}

func TestPersistConfig_Overwrite(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	// First write
	err := persistConfig(store, &commonpb.PersistedConfig{
		NodeId:    1,
		ClusterId: "cluster-a",
	})
	require.NoError(t, err)

	// Overwrite
	err = persistConfig(store, &commonpb.PersistedConfig{
		NodeId:    2,
		ClusterId: "cluster-b",
	})
	require.NoError(t, err)

	loaded, err := LoadPersistedConfig(store)
	require.NoError(t, err)
	require.NotNil(t, loaded)
	assert.Equal(t, uint64(2), loaded.GetNodeId())
	assert.Equal(t, "cluster-b", loaded.GetClusterId())
}
