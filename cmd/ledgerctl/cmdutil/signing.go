package cmdutil

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/formancehq/ledger-v3-poc/internal/crypto/signing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/spf13/cobra"
)

// LoadSigningKey loads the signing key and key ID from command flags.
// Returns empty values if no signing key is configured.
func LoadSigningKey(cmd *cobra.Command) (string, ed25519.PrivateKey, error) {
	keyPath, _ := cmd.Flags().GetString("signing-key")
	if keyPath == "" {
		return "", nil, nil
	}

	keyID, _ := cmd.Flags().GetString("signing-key-id")
	if keyID == "" {
		keyID = "default"
	}

	// Read the seed file
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to read signing key file: %w", err)
	}

	// Try to interpret as hex-encoded seed
	seed := data
	trimmed := strings.TrimSpace(string(data))
	if decoded, err := hex.DecodeString(trimmed); err == nil && len(decoded) == ed25519.SeedSize {
		seed = decoded
	}

	if len(seed) != ed25519.SeedSize {
		return "", nil, fmt.Errorf("signing key seed must be %d bytes, got %d", ed25519.SeedSize, len(seed))
	}

	return keyID, ed25519.NewKeyFromSeed(seed), nil
}

// SignRequests signs each request using the signing key from command flags.
// If no signing key is configured, this is a no-op.
func SignRequests(cmd *cobra.Command, requests []*servicepb.Request) error {
	keyID, privKey, err := LoadSigningKey(cmd)
	if err != nil {
		return err
	}
	if privKey == nil {
		return nil
	}

	for _, req := range requests {
		if err := signing.Sign(req, keyID, privKey); err != nil {
			return fmt.Errorf("failed to sign request: %w", err)
		}
	}
	return nil
}
