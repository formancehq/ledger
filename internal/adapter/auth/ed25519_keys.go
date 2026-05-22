package auth

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"

	jose "github.com/go-jose/go-jose/v4"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"

	"github.com/formancehq/ledger-v3-poc/internal/domain/crypto/signing"
)

// Ed25519KeyEntry represents a single Ed25519 public key entry in the authentication config file.
type Ed25519KeyEntry struct {
	KeyID         string   `json:"keyId"`
	PublicKeyFile string   `json:"publicKeyFile"`
	Scopes        []string `json:"scopes"`
	God           bool     `json:"god"`
}

// Ed25519KeysConfig is the top-level structure for the Ed25519 keys JSON config file.
type Ed25519KeysConfig struct {
	Keys []Ed25519KeyEntry `json:"keys"`
}

// Ed25519KeySetResult holds the results of loading Ed25519 keys.
type Ed25519KeySetResult struct {
	KeySet        *oidc.StaticKeySet
	AllowedScopes map[string][]string
	GodKeys       map[string]bool
}

// LoadEd25519KeySet loads Ed25519 public keys from a JSON config file and returns
// a StaticKeySet for JWT signature verification, a map of keyID -> allowed scopes,
// and a set of key IDs that are allowed to emit god-mode tokens.
func LoadEd25519KeySet(configPath string) (Ed25519KeySetResult, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return Ed25519KeySetResult{}, fmt.Errorf("reading Ed25519 keys config: %w", err)
	}

	var cfg Ed25519KeysConfig
	if err := json.Unmarshal(data, &cfg); err != nil {
		return Ed25519KeySetResult{}, fmt.Errorf("parsing Ed25519 keys config: %w", err)
	}

	if len(cfg.Keys) == 0 {
		return Ed25519KeySetResult{}, errors.New("Ed25519 keys config contains no keys")
	}

	var (
		jwks          []jose.JSONWebKey
		allowedScopes = make(map[string][]string, len(cfg.Keys))
		godKeys       = make(map[string]bool)
	)

	for _, entry := range cfg.Keys {
		if entry.KeyID == "" {
			return Ed25519KeySetResult{}, errors.New("Ed25519 key entry missing keyId")
		}

		if entry.PublicKeyFile == "" {
			return Ed25519KeySetResult{}, fmt.Errorf("Ed25519 key %q missing publicKeyFile", entry.KeyID)
		}

		pubKey, err := signing.LoadPublicKeyFromFile(entry.PublicKeyFile)
		if err != nil {
			return Ed25519KeySetResult{}, fmt.Errorf("loading public key for %q: %w", entry.KeyID, err)
		}

		jwks = append(jwks, jose.JSONWebKey{
			Key:       pubKey,
			KeyID:     entry.KeyID,
			Algorithm: string(jose.EdDSA),
			Use:       "sig",
		})

		allowedScopes[entry.KeyID] = entry.Scopes

		if entry.God {
			godKeys[entry.KeyID] = true
		}
	}

	return Ed25519KeySetResult{
		KeySet:        oidc.NewStaticKeySet(jwks...),
		AllowedScopes: allowedScopes,
		GodKeys:       godKeys,
	}, nil
}

// enforceAllowedScopes checks that all claimed scopes are permitted by the key's allowed scopes.
// Returns an error if any claimed scope exceeds the key's allowlist.
// God-mode keys skip scope enforcement entirely.
func enforceAllowedScopes(claimed []string, keyID string, allowed map[string][]string, godKeys map[string]bool) error {
	if _, ok := allowed[keyID]; !ok {
		return fmt.Errorf("unknown key ID %q", keyID)
	}

	// God-mode keys are allowed to claim any scope.
	if godKeys[keyID] {
		return nil
	}

	keyScopes := allowed[keyID]
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

// enforceGodClaim checks that a token claiming god mode was signed by a key
// that is allowed to emit god-mode tokens. Returns an error if the key is not
// in the godKeys set.
func enforceGodClaim(keyID string, godKeys map[string]bool) error {
	if !godKeys[keyID] {
		return fmt.Errorf("key %q is not allowed to claim god mode", keyID)
	}

	return nil
}
