package bootstrap

import (
	"crypto/ed25519"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
)

func TestBuildAuthConfig_DuplicateEd25519KeyIDs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	publicKeyPath := filepath.Join(dir, "public-key.hex")
	privateKey := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	publicKey, ok := privateKey.Public().(ed25519.PublicKey)
	require.True(t, ok)
	require.NoError(t, os.WriteFile(publicKeyPath, []byte(hex.EncodeToString(publicKey)), 0600))

	configPath := filepath.Join(dir, "auth-keys.json")
	data, err := json.Marshal(internalauth.Ed25519KeysConfig{
		Keys: []internalauth.Ed25519KeyEntry{
			{KeyID: "shared", PublicKeyFile: publicKeyPath, Scopes: []string{"ledger:read"}},
			{KeyID: "shared", PublicKeyFile: publicKeyPath, Superuser: true},
		},
	})
	require.NoError(t, err)
	require.NoError(t, os.WriteFile(configPath, data, 0600))

	authCfg, err := buildAuthConfig(Config{
		AuthConfig: AuthFlagConfig{Enabled: true, Ed25519KeysFile: configPath},
	}, logging.Testing(), nil)

	require.EqualError(t, err, `loading Ed25519 keys: Ed25519 keys config contains duplicate keyId "shared"`)
	require.Nil(t, authCfg.KeySet, "a rejected static keyset must not be installed")
	require.Nil(t, authCfg.Ed25519AllowedScopes)
	require.Nil(t, authCfg.Ed25519SuperuserKeys)
}
