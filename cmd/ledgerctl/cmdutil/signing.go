package cmdutil

import (
	"crypto/ed25519"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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

	seed, err := signing.LoadSeedFromFile(keyPath)
	if err != nil {
		return "", nil, fmt.Errorf("failed to load signing key: %w", err)
	}

	return keyID, ed25519.NewKeyFromSeed(seed), nil
}

// LoadResponseVerifyKey loads the Ed25519 public key for response signature verification.
// Returns nil if --response-verify-key is not set.
func LoadResponseVerifyKey(cmd *cobra.Command) (ed25519.PublicKey, error) {
	keyPath, _ := cmd.Flags().GetString("response-verify-key")
	if keyPath == "" {
		return nil, nil
	}

	pubKey, err := signing.LoadPublicKeyFromFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load response verify key: %w", err)
	}

	return pubKey, nil
}

// VerifyResponseSignatures verifies the response signatures on the given logs.
// If no verify key is configured (--response-verify-key), this is a no-op.
func VerifyResponseSignatures(cmd *cobra.Command, logs []*commonpb.Log) error {
	pubKey, err := LoadResponseVerifyKey(cmd)
	if err != nil {
		return err
	}

	if pubKey == nil {
		return nil
	}

	for _, log := range logs {
		if log.GetResponseSignature() == nil {
			return fmt.Errorf("log %d: missing response signature (server may not have response signing enabled)", log.GetSequence())
		}

		err := signing.VerifyResponseSignature(log.GetResponseSignature(), pubKey)
		if err != nil {
			return fmt.Errorf("log %d: %w", log.GetSequence(), err)
		}
	}

	return nil
}

// BuildApplyRequest assembles the requests into one atomic ApplyBatch and
// returns the ApplyRequest to pass to Apply — signed as a whole when a signing
// key is configured on the command flags, unsigned otherwise. Signing the batch
// authenticates its composition and ordering.
func BuildApplyRequest(cmd *cobra.Command, requests ...*servicepb.Request) (*servicepb.ApplyRequest, error) {
	keyID, privKey, err := LoadSigningKey(cmd)
	if err != nil {
		return nil, err
	}

	if privKey == nil {
		return servicepb.UnsignedApplyRequest("", requests...), nil
	}

	sb, err := signing.Sign(&servicepb.ApplyBatch{Requests: requests}, keyID, privKey)
	if err != nil {
		return nil, fmt.Errorf("failed to sign batch: %w", err)
	}

	return servicepb.SignedApplyRequest(sb), nil
}
