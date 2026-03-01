package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func writeTestKey(t *testing.T, dir, name string) ed25519.PublicKey {
	t.Helper()
	seed := make([]byte, ed25519.SeedSize)
	_, err := rand.Read(seed)
	require.NoError(t, err)

	privKey := ed25519.NewKeyFromSeed(seed)
	pubKey := privKey.Public().(ed25519.PublicKey)

	pubKeyPath := filepath.Join(dir, name+".pubkey.hex")
	err = os.WriteFile(pubKeyPath, []byte(hex.EncodeToString(pubKey)+"\n"), 0644)
	require.NoError(t, err)

	return pubKey
}

func TestLoadEd25519KeySet_Valid(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestKey(t, dir, "key1")
	writeTestKey(t, dir, "key2")

	configPath := filepath.Join(dir, "auth-keys.json")
	config := `{
		"keys": [
			{"keyId": "key1", "publicKeyFile": "` + filepath.Join(dir, "key1.pubkey.hex") + `", "scopes": ["ledger:read"]},
			{"keyId": "key2", "publicKeyFile": "` + filepath.Join(dir, "key2.pubkey.hex") + `", "scopes": ["ledger:read", "ledger:write"]}
		]
	}`
	err := os.WriteFile(configPath, []byte(config), 0644)
	require.NoError(t, err)

	keySet, allowedScopes, err := LoadEd25519KeySet(configPath)
	require.NoError(t, err)
	require.NotNil(t, keySet)
	require.Len(t, allowedScopes, 2)
	assert.Equal(t, []string{"ledger:read"}, allowedScopes["key1"])
	assert.Equal(t, []string{"ledger:read", "ledger:write"}, allowedScopes["key2"])
}

func TestLoadEd25519KeySet_MissingFile(t *testing.T) {
	t.Parallel()

	_, _, err := LoadEd25519KeySet("/nonexistent/path.json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading Ed25519 keys config")
}

func TestLoadEd25519KeySet_EmptyKeys(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "auth-keys.json")
	err := os.WriteFile(configPath, []byte(`{"keys": []}`), 0644)
	require.NoError(t, err)

	_, _, err = LoadEd25519KeySet(configPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "contains no keys")
}

func TestLoadEd25519KeySet_MissingKeyID(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestKey(t, dir, "k")

	configPath := filepath.Join(dir, "auth-keys.json")
	config := `{"keys": [{"keyId": "", "publicKeyFile": "` + filepath.Join(dir, "k.pubkey.hex") + `", "scopes": []}]}`
	err := os.WriteFile(configPath, []byte(config), 0644)
	require.NoError(t, err)

	_, _, err = LoadEd25519KeySet(configPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing keyId")
}

func TestEnforceAllowedScopes_Valid(t *testing.T) {
	t.Parallel()

	allowed := map[string][]string{
		"bot": {"ledger:read", "ledger:write"},
	}

	err := enforceAllowedScopes([]string{"ledger:read"}, "bot", allowed)
	require.NoError(t, err)

	err = enforceAllowedScopes([]string{"ledger:read", "ledger:write"}, "bot", allowed)
	require.NoError(t, err)
}

func TestEnforceAllowedScopes_ExcessiveScope(t *testing.T) {
	t.Parallel()

	allowed := map[string][]string{
		"bot": {"ledger:read"},
	}

	err := enforceAllowedScopes([]string{"ledger:read", "ledger:admin"}, "bot", allowed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ledger:admin")
	assert.Contains(t, err.Error(), "not allowed")
}

func TestEnforceAllowedScopes_UnknownKey(t *testing.T) {
	t.Parallel()

	allowed := map[string][]string{
		"bot": {"ledger:read"},
	}

	err := enforceAllowedScopes([]string{"ledger:read"}, "unknown-key", allowed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown key ID")
}
