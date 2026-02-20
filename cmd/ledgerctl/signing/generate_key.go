package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"
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

	if err := os.MkdirAll(outputDir, 0700); err != nil {
		return fmt.Errorf("creating output directory: %w", err)
	}

	seed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(seed); err != nil {
		return fmt.Errorf("generating random seed: %w", err)
	}

	privKey := ed25519.NewKeyFromSeed(seed)
	pubKey := privKey.Public().(ed25519.PublicKey)

	seedPath := filepath.Join(outputDir, "seed.hex")
	if err := os.WriteFile(seedPath, []byte(hex.EncodeToString(seed)+"\n"), 0600); err != nil {
		return fmt.Errorf("writing seed file: %w", err)
	}

	pubKeyPath := filepath.Join(outputDir, "pubkey.hex")
	if err := os.WriteFile(pubKeyPath, []byte(hex.EncodeToString(pubKey)+"\n"), 0644); err != nil {
		return fmt.Errorf("writing public key file: %w", err)
	}

	fmt.Printf("Ed25519 keypair generated in %s/\n", outputDir)
	fmt.Printf("  Seed (private):  %s  (mode 0600)\n", seedPath)
	fmt.Printf("  Public key:      %s\n", pubKeyPath)
	fmt.Println()
	fmt.Println("Usage:")
	fmt.Printf("  ledgerctl signing register-key --key-id <id> --public-key-file %s\n", pubKeyPath)
	fmt.Printf("  ledgerctl --signing-key %s ledgers create --name my-ledger\n", seedPath)

	return nil
}
