package auth

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
)

// NewGenerateKeyCommand returns the "auth generate-key" command.
func NewGenerateKeyCommand() *cobra.Command {
	return &cobra.Command{
		Use:   "generate-key <output-directory>",
		Short: "Generate an Ed25519 keypair for JWT authentication",
		Long: `Generate an Ed25519 keypair for use with --auth-ed25519-keys on the server.

Creates two files:
  - seed.hex:   32-byte Ed25519 seed (hex-encoded), used with --signing-key or generate-token
  - pubkey.hex: 32-byte Ed25519 public key (hex-encoded), referenced in auth-keys.json`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			outputDir := args[0]

			keyID, err := signing.GenerateKeyPair(outputDir)
			if err != nil {
				return err
			}

			seedPath := filepath.Join(outputDir, "seed.hex")
			pubKeyPath := filepath.Join(outputDir, "pubkey.hex")

			fmt.Printf("Ed25519 keypair generated in %s/\n", outputDir)
			fmt.Printf("  Key ID:          %s\n", keyID)
			fmt.Printf("  Seed (private):  %s  (mode 0600)\n", seedPath)
			fmt.Printf("  Public key:      %s\n", pubKeyPath)
			fmt.Println()
			fmt.Println("Add to auth-keys.json:")
			fmt.Printf("  {\"keyId\": \"%s\", \"publicKeyFile\": \"%s\", \"scopes\": [\"ledger:read\", \"ledger:write\"]}\n", keyID, pubKeyPath)
			fmt.Println()
			fmt.Println("Generate a token:")
			fmt.Printf("  ledgerctl auth generate-token --signing-key %s --key-id %s --subject my-service\n", seedPath, keyID)

			return nil
		},
	}
}
