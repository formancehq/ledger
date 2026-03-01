package auth

import (
	"crypto/ed25519"
	"encoding/json"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v3/oidc"
	jose "github.com/go-jose/go-jose/v4"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
	"github.com/spf13/cobra"
)

// NewGenerateTokenCommand returns the "auth generate-token" command.
func NewGenerateTokenCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "generate-token",
		Short: "Generate a signed EdDSA JWT token",
		Long: `Generate a JWT token signed with an Ed25519 key for use with servers
configured with --auth-ed25519-keys.

The token is output on stdout and can be used with --auth-token or the
Authorization: Bearer header.`,
		RunE: runGenerateToken,
	}

	cmd.Flags().String("signing-key", "", "Path to Ed25519 seed file (required)")
	cmd.Flags().String("key-id", "", "Key ID matching the server's auth-keys.json (required)")
	cmd.Flags().String("subject", "", "JWT subject (required)")
	cmd.Flags().StringSlice("scopes", nil, "Scopes to include (e.g., ledger:read,ledger:write)")
	cmd.Flags().Duration("expiration", 1*time.Hour, "Token validity duration")

	_ = cmd.MarkFlagRequired("signing-key")
	_ = cmd.MarkFlagRequired("key-id")
	_ = cmd.MarkFlagRequired("subject")

	return cmd
}

func runGenerateToken(cmd *cobra.Command, _ []string) error {
	signingKeyPath, _ := cmd.Flags().GetString("signing-key")
	keyID, _ := cmd.Flags().GetString("key-id")
	subject, _ := cmd.Flags().GetString("subject")
	scopes, _ := cmd.Flags().GetStringSlice("scopes")
	expiration, _ := cmd.Flags().GetDuration("expiration")

	seed, err := signing.LoadSeedFromFile(signingKeyPath)
	if err != nil {
		return fmt.Errorf("loading signing key: %w", err)
	}

	privKey := ed25519.NewKeyFromSeed(seed)

	now := time.Now()
	claims := &oidc.AccessTokenClaims{}
	claims.Subject = subject
	claims.IssuedAt = oidc.FromTime(oidc.Time(now.Unix()).AsTime())
	claims.Expiration = oidc.FromTime(oidc.Time(now.Add(expiration).Unix()).AsTime())
	claims.Scopes = oidc.SpaceDelimitedArray(scopes)

	payload, err := json.Marshal(claims)
	if err != nil {
		return fmt.Errorf("marshaling claims: %w", err)
	}

	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.EdDSA,
		Key:       &jose.JSONWebKey{Key: privKey, KeyID: keyID},
	}, nil)
	if err != nil {
		return fmt.Errorf("creating signer: %w", err)
	}

	jws, err := signer.Sign(payload)
	if err != nil {
		return fmt.Errorf("signing token: %w", err)
	}

	token, err := jws.CompactSerialize()
	if err != nil {
		return fmt.Errorf("serializing token: %w", err)
	}

	fmt.Print(token)
	return nil
}
