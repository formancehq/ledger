package auth

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/formancehq/go-libs/v3/oidc"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
)

// Ed25519KeyEntry represents a single Ed25519 public key entry in the authentication config file.
type Ed25519KeyEntry struct {
	KeyID         string   `json:"keyId"`
	PublicKeyFile string   `json:"publicKeyFile"`
	Scopes        []string `json:"scopes"`
}

// Ed25519KeysConfig is the top-level structure for the Ed25519 keys JSON config file.
type Ed25519KeysConfig struct {
	Keys []Ed25519KeyEntry `json:"keys"`
}

// LoadEd25519KeySet loads Ed25519 public keys from a JSON config file and returns
// a StaticKeySet for JWT signature verification, plus a map of keyID -> allowed scopes.
func LoadEd25519KeySet(configPath string) (*oidc.StaticKeySet, map[string][]string, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, nil, fmt.Errorf("reading Ed25519 keys config: %w", err)
	}

	var cfg Ed25519KeysConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, nil, fmt.Errorf("parsing Ed25519 keys config: %w", err)
	}

	if len(cfg.Keys) == 0 {
		return nil, nil, fmt.Errorf("Ed25519 keys config contains no keys")
	}

	var (
		jwks          []jose.JSONWebKey
		allowedScopes = make(map[string][]string, len(cfg.Keys))
	)

	for _, entry := range cfg.Keys {
		if entry.KeyID == "" {
			return nil, nil, fmt.Errorf("Ed25519 key entry missing keyId")
		}
		if entry.PublicKeyFile == "" {
			return nil, nil, fmt.Errorf("Ed25519 key %q missing publicKeyFile", entry.KeyID)
		}

		pubKey, err := signing.LoadPublicKeyFromFile(entry.PublicKeyFile)
		if err != nil {
			return nil, nil, fmt.Errorf("loading public key for %q: %w", entry.KeyID, err)
		}

		jwks = append(jwks, jose.JSONWebKey{
			Key:       pubKey,
			KeyID:     entry.KeyID,
			Algorithm: string(jose.EdDSA),
			Use:       "sig",
		})

		allowedScopes[entry.KeyID] = entry.Scopes
	}

	return oidc.NewStaticKeySet(jwks...), allowedScopes, nil
}

// enforceAllowedScopes checks that all claimed scopes are permitted by the key's allowed scopes.
// Returns an error if any claimed scope exceeds the key's allowlist.
func enforceAllowedScopes(claimed []string, keyID string, allowed map[string][]string) error {
	keyScopes, ok := allowed[keyID]
	if !ok {
		return fmt.Errorf("unknown key ID %q", keyID)
	}

	allowedSet := make(map[string]struct{}, len(keyScopes))
	for _, s := range keyScopes {
		allowedSet[s] = struct{}{}
	}

	for _, scope := range claimed {
		if _, found := allowedSet[scope]; !found {
			return fmt.Errorf("scope %q not allowed for key %q", scope, keyID)
		}
	}

	return nil
}
