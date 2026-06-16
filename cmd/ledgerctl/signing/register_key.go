package signing

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// NewRegisterKeyCommand creates the signing register-key command.
func NewRegisterKeyCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:     "register-key",
		Aliases: []string{"add-key", "register"},
		Short:   "Register an Ed25519 public key for signature verification",
		Long: `Register an Ed25519 public key for signature verification.

The first key registration can be unsigned (bootstrap). Once keys exist,
this command must be signed by an existing key (use --signing-key).

The public key can be provided as:
  - A hex-encoded string via --public-key flag
  - A file path via --public-key-file flag (raw 32 bytes or hex-encoded)

Examples:
  # Bootstrap: register the first key (unsigned)
  ledgerctl signing register-key --key-id admin --public-key-file /path/to/pubkey

  # Register additional key (must be signed)
  ledgerctl signing register-key --key-id ops --public-key <hex> --signing-key /path/to/seed`,
		Args: cobra.NoArgs,
		RunE: runRegisterKey,
	}

	cmd.Flags().String("key-id", "", "Unique identifier for the key (required)")
	cmd.Flags().String("public-key", "", "Ed25519 public key as hex-encoded string (32 bytes)")
	cmd.Flags().String("public-key-file", "", "Path to file containing Ed25519 public key (raw 32 bytes or hex-encoded)")
	cmdutil.AddOutputFlags(cmd)
	cmd.Flags().Duration("timeout", cmdutil.DefaultTimeout, "Request timeout")

	return cmd
}

func runRegisterKey(cmd *cobra.Command, _ []string) error {
	keyID, _ := cmd.Flags().GetString("key-id")
	if keyID == "" {
		return errors.New("--key-id is required")
	}

	pubKey, err := loadPublicKey(cmd)
	if err != nil {
		return err
	}

	client, conn, err := cmdutil.GetClient(cmd)
	if err != nil {
		return err
	}

	defer func() { _ = conn.Close() }()

	ctx, cancel := cmdutil.GetContext(cmd)
	defer cancel()

	spinner, _ := pterm.DefaultSpinner.Start(fmt.Sprintf("Registering signing key %s...", keyID))

	requests := []*servicepb.Request{
		{
			Type: &servicepb.Request_RegisterSigningKey{
				RegisterSigningKey: &servicepb.RegisterSigningKeyRequest{
					KeyId:     keyID,
					PublicKey: []byte(pubKey),
				},
			},
		},
	}

	envelopes, err := cmdutil.BuildEnvelopes(cmd, requests)
	if err != nil {
		spinner.Fail("Failed to sign request")

		return cmdutil.Displayed(err)
	}

	_, err = client.Apply(ctx, &servicepb.ApplyRequest{Envelopes: envelopes})
	if err != nil {
		_ = spinner.Stop()

		return cmdutil.FormatGRPCError("failed to register signing key", err)
	}

	spinner.Success("Registered")

	if handled, err := cmdutil.EncodeStructured(cmd, map[string]any{"keyId": keyID, "publicKey": hex.EncodeToString(pubKey)}); handled || err != nil {
		return err
	}

	pterm.Println()
	pterm.Printf("Key ID:     %s\n", pterm.Cyan(keyID))
	pterm.Printf("Public Key: %s\n", pterm.Gray(hex.EncodeToString(pubKey)))

	return nil
}

// loadPublicKey loads an Ed25519 public key from --public-key (hex) or --public-key-file.
func loadPublicKey(cmd *cobra.Command) (ed25519.PublicKey, error) {
	hexKey, _ := cmd.Flags().GetString("public-key")
	filePath, _ := cmd.Flags().GetString("public-key-file")

	if hexKey == "" && filePath == "" {
		return nil, errors.New("either --public-key or --public-key-file is required")
	}

	if hexKey != "" {
		decoded, err := hex.DecodeString(strings.TrimSpace(hexKey))
		if err != nil {
			return nil, fmt.Errorf("invalid hex public key: %w", err)
		}

		if len(decoded) != ed25519.PublicKeySize {
			return nil, fmt.Errorf("public key must be %d bytes, got %d", ed25519.PublicKeySize, len(decoded))
		}

		return ed25519.PublicKey(decoded), nil
	}

	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read public key file: %w", err)
	}

	// Try hex-encoded first
	trimmed := strings.TrimSpace(string(data))
	if decoded, err := hex.DecodeString(trimmed); err == nil && len(decoded) == ed25519.PublicKeySize {
		return ed25519.PublicKey(decoded), nil
	}

	// Try raw bytes
	if len(data) == ed25519.PublicKeySize {
		return ed25519.PublicKey(data), nil
	}

	return nil, fmt.Errorf("public key file must contain %d bytes (raw or hex-encoded), got %d bytes", ed25519.PublicKeySize, len(data))
}
