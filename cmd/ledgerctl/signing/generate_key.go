package signing

import (
	"fmt"
	"path/filepath"

	"github.com/spf13/cobra"

	cryptosigning "github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
)

func NewGenerateKeyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "generate-key <output-directory>",
		Aliases: []string{"gen-key", "keygen"},
		Short:   "Generate an Ed25519 keypair for request signing",
		Long: `Generate an Ed25519 keypair and write the seed and public key to the specified directory.

Creates two files:
  - seed.hex:   32-byte Ed25519 seed (hex-encoded), used with --signing-key
  - pubkey.hex: 32-byte Ed25519 public key (hex-encoded), used with signing register-key`,
		Args: cobra.ExactArgs(1),
		RunE: runGenerateKey,
	}

	return cmd
}

func runGenerateKey(_ *cobra.Command, args []string) error {
	outputDir := args[0]

	keyID, err := cryptosigning.GenerateKeyPair(outputDir)
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
	fmt.Println("Usage:")
	fmt.Printf("  ledgerctl signing register-key --key-id <id> --public-key-file %s\n", pubKeyPath)
	fmt.Printf("  ledgerctl --signing-key %s ledgers create --name my-ledger\n", seedPath)

	return nil
}
