package auth

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
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
	pubKey, ok := privKey.Public().(ed25519.PublicKey)
	require.True(t, ok, "ed25519 private key must produce ed25519.PublicKey")

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

	result, err := LoadEd25519KeySet(configPath)
	require.NoError(t, err)
	require.NotNil(t, result.KeySet)
	require.Len(t, result.AllowedScopes, 2)
	assert.Equal(t, []string{"ledger:read"}, result.AllowedScopes["key1"])
	assert.Equal(t, []string{"ledger:read", "ledger:write"}, result.AllowedScopes["key2"])
	assert.Empty(t, result.SuperuserKeys)
}

func TestLoadEd25519KeySet_SuperuserKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	writeTestKey(t, dir, "admin")
	writeTestKey(t, dir, "bot")

	configPath := filepath.Join(dir, "auth-keys.json")
	config := `{
		"keys": [
			{"keyId": "admin", "publicKeyFile": "` + filepath.Join(dir, "admin.pubkey.hex") + `", "scopes": [], "superuser": true},
			{"keyId": "bot", "publicKeyFile": "` + filepath.Join(dir, "bot.pubkey.hex") + `", "scopes": ["ledger:read"]}
		]
	}`
	err := os.WriteFile(configPath, []byte(config), 0644)
	require.NoError(t, err)

	result, err := LoadEd25519KeySet(configPath)
	require.NoError(t, err)
	assert.True(t, result.SuperuserKeys["admin"])
	assert.False(t, result.SuperuserKeys["bot"])
}

func TestLoadEd25519KeySet_MissingFile(t *testing.T) {
	t.Parallel()

	_, err := LoadEd25519KeySet("/nonexistent/path.json")
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading Ed25519 keys config")
}

func TestLoadEd25519KeySet_EmptyKeys(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	configPath := filepath.Join(dir, "auth-keys.json")
	err := os.WriteFile(configPath, []byte(`{"keys": []}`), 0644)
	require.NoError(t, err)

	_, err = LoadEd25519KeySet(configPath)
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

	_, err = LoadEd25519KeySet(configPath)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "missing keyId")
}

func TestEnforceAllowedScopes_Valid(t *testing.T) {
	t.Parallel()

	allowed := map[string][]string{
		"bot": {"ledger:read", "ledger:write"},
	}

	err := enforceAllowedScopes([]string{"ledger:read"}, "bot", allowed, nil, DefaultMapping("ledger"))
	require.NoError(t, err)

	err = enforceAllowedScopes([]string{"ledger:read", "ledger:write"}, "bot", allowed, nil, DefaultMapping("ledger"))
	require.NoError(t, err)
}

func TestEnforceAllowedScopes_ExcessiveScope(t *testing.T) {
	t.Parallel()

	allowed := map[string][]string{
		"bot": {"ledger:read"},
	}

	err := enforceAllowedScopes([]string{"ledger:read", "ledger:admin"}, "bot", allowed, nil, DefaultMapping("ledger"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "ledger:admin")
	assert.Contains(t, err.Error(), "not allowed")
}

func TestEnforceAllowedScopes_GranularScopeCoveredByVirtualScope(t *testing.T) {
	t.Parallel()

	allowed := map[string][]string{
		"reader": {"ledger:read"},
	}

	// A least-privilege token claims one granular scope that "ledger:read" expands to.
	err := enforceAllowedScopes([]string{"ledger:ReadTransaction"}, "reader", allowed, nil, DefaultMapping("ledger"))
	require.NoError(t, err)
}

func TestEnforceAllowedScopes_GranularScopeOutsideVirtualScope(t *testing.T) {
	t.Parallel()

	allowed := map[string][]string{
		"reader": {"ledger:read"},
	}

	err := enforceAllowedScopes([]string{"ledger:WriteTransaction"}, "reader", allowed, nil, DefaultMapping("ledger"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `scope "ledger:WriteTransaction" not allowed for key "reader"`)
}

func TestEnforceAllowedScopes_UnknownScopeRejected(t *testing.T) {
	t.Parallel()

	allowed := map[string][]string{
		"reader": {"ledger:read"},
	}

	// An unknown scope expands to nothing; it must not be treated as covered.
	err := enforceAllowedScopes([]string{"ledger:unknown"}, "reader", allowed, nil, DefaultMapping("ledger"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `scope "ledger:unknown" not allowed for key "reader"`)
}

func TestEnforceAllowedScopes_VirtualScopeNotCoveredByPartialGranularAllowlist(t *testing.T) {
	t.Parallel()

	allowed := map[string][]string{
		"narrow": {"ledger:ReadTransaction"},
	}

	// "ledger:read" expands to more than the key allows.
	err := enforceAllowedScopes([]string{"ledger:read"}, "narrow", allowed, nil, DefaultMapping("ledger"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), `scope "ledger:read" not allowed for key "narrow"`)
}

func TestEnforceAllowedScopes_UnknownKey(t *testing.T) {
	t.Parallel()

	allowed := map[string][]string{
		"bot": {"ledger:read"},
	}

	err := enforceAllowedScopes([]string{"ledger:read"}, "unknown-key", allowed, nil, DefaultMapping("ledger"))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown key ID")
}

func TestEnforceAllowedScopes_SuperuserKeyBypassesCheck(t *testing.T) {
	t.Parallel()

	allowed := map[string][]string{
		"admin": {},
	}
	superuserKeys := map[string]bool{"admin": true}

	// Superuser key can claim any scope regardless of allowlist.
	err := enforceAllowedScopes([]string{"ledger:admin", "ledger:write"}, "admin", allowed, superuserKeys, DefaultMapping("ledger"))
	require.NoError(t, err)
}

func TestEnforceSuperuserClaim_Allowed(t *testing.T) {
	t.Parallel()

	superuserKeys := map[string]bool{"admin": true}

	err := enforceSuperuserClaim("admin", superuserKeys)
	require.NoError(t, err)
}

func TestEnforceSuperuserClaim_NotAllowed(t *testing.T) {
	t.Parallel()

	superuserKeys := map[string]bool{"admin": true}

	err := enforceSuperuserClaim("bot", superuserKeys)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not allowed to claim superuser mode")
}

func TestLoadEd25519KeySet_DuplicateKeyID(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name                 string
		firstScopes          []string
		secondScopes         []string
		firstSuperuser       bool
		secondSuperuser      bool
		sameKey              bool
		nonadjacent          bool
		invalidDuplicateFile bool
	}{
		{name: "conflicting scopes", firstScopes: []string{"ledger:read"}, secondScopes: []string{"ledger:read", "ledger:admin"}},
		{name: "later superuser", firstScopes: []string{"ledger:read"}, secondSuperuser: true},
		{name: "earlier superuser", firstSuperuser: true, secondScopes: []string{"ledger:read"}},
		{name: "identical entries", sameKey: true},
		{name: "empty scopes", firstScopes: []string{}, secondScopes: []string{}},
		{name: "nonadjacent duplicate", nonadjacent: true},
		{name: "duplicate precedes file validation", invalidDuplicateFile: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			writeTestKey(t, dir, "first")
			writeTestKey(t, dir, "second")
			firstFile := filepath.Join(dir, "first.pubkey.hex")
			secondFile := filepath.Join(dir, "second.pubkey.hex")
			if tc.sameKey {
				secondFile = firstFile
			}
			if tc.invalidDuplicateFile {
				secondFile = filepath.Join(dir, "missing.hex")
			}
			entries := []Ed25519KeyEntry{{KeyID: "dup", PublicKeyFile: firstFile, Scopes: tc.firstScopes, Superuser: tc.firstSuperuser}}
			if tc.nonadjacent {
				entries = append(entries, Ed25519KeyEntry{KeyID: "other", PublicKeyFile: firstFile})
			}
			entries = append(entries, Ed25519KeyEntry{KeyID: "dup", PublicKeyFile: secondFile, Scopes: tc.secondScopes, Superuser: tc.secondSuperuser})
			data, err := json.Marshal(Ed25519KeysConfig{Keys: entries})
			require.NoError(t, err)
			configPath := filepath.Join(dir, "auth-keys.json")
			require.NoError(t, os.WriteFile(configPath, data, 0600))

			result, err := LoadEd25519KeySet(configPath)
			require.EqualError(t, err, `Ed25519 keys config contains duplicate keyId "dup"`)
			require.Equal(t, Ed25519KeySetResult{}, result)
		})
	}
}
