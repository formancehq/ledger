package auth

import (
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/pterm/pterm"
	"github.com/spf13/cobra"

	"github.com/formancehq/go-libs/v3/oidc"

	"github.com/formancehq/ledger-v3-poc/cmd/ledgerctl/cmdutil"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/crypto/signing"
)

// tokenParams holds the parameters for signing a JWT token.
type tokenParams struct {
	seed       []byte
	keyID      string
	subject    string
	scopes     []string
	expiration time.Duration
}

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

	addTokenGenerationFlags(cmd)
	cmd.Flags().Bool("store", false, "Store the generated token in the OS keychain (keyed by --server)")

	return cmd
}

// addTokenGenerationFlags adds the common flags for Ed25519 token generation.
// Flags are not marked as required here; callers validate as needed.
func addTokenGenerationFlags(cmd *cobra.Command) {
	cmd.Flags().String("signing-key", "", "Path to Ed25519 seed file")
	cmd.Flags().String("key-id", "", "Key ID matching the server's auth-keys.json")
	cmd.Flags().String("subject", "", "JWT subject")
	cmd.Flags().StringSlice("scopes", nil, "Scopes to include (e.g., ledger:read,ledger:write)")
	cmd.Flags().Duration("expiration", 1*time.Hour, "Token validity duration")
}

// signToken creates a signed JWT from the given parameters.
func signToken(p tokenParams) (string, error) {
	privKey := ed25519.NewKeyFromSeed(p.seed)

	now := time.Now()
	claims := &oidc.AccessTokenClaims{}
	claims.Subject = p.subject
	claims.IssuedAt = oidc.FromTime(oidc.Time(now.Unix()).AsTime())
	claims.Expiration = oidc.FromTime(oidc.Time(now.Add(p.expiration).Unix()).AsTime())
	claims.Scopes = oidc.SpaceDelimitedArray(p.scopes)

	payload, err := json.Marshal(claims)
	if err != nil {
		return "", fmt.Errorf("marshaling claims: %w", err)
	}

	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.EdDSA,
		Key:       &jose.JSONWebKey{Key: privKey, KeyID: p.keyID},
	}, nil)
	if err != nil {
		return "", fmt.Errorf("creating signer: %w", err)
	}

	jws, err := signer.Sign(payload)
	if err != nil {
		return "", fmt.Errorf("signing token: %w", err)
	}

	token, err := jws.CompactSerialize()
	if err != nil {
		return "", fmt.Errorf("serializing token: %w", err)
	}

	return token, nil
}

// tokenParamsFromFlags reads flag values, loads the seed from file, and returns tokenParams.
func tokenParamsFromFlags(cmd *cobra.Command) (tokenParams, error) {
	signingKeyPath, _ := cmd.Flags().GetString("signing-key")
	if signingKeyPath == "" {
		return tokenParams{}, errors.New("required flag \"signing-key\" not set")
	}

	keyID, _ := cmd.Flags().GetString("key-id")
	if keyID == "" {
		return tokenParams{}, errors.New("required flag \"key-id\" not set")
	}

	subject, _ := cmd.Flags().GetString("subject")
	if subject == "" {
		return tokenParams{}, errors.New("required flag \"subject\" not set")
	}

	scopes, _ := cmd.Flags().GetStringSlice("scopes")
	expiration, _ := cmd.Flags().GetDuration("expiration")

	seed, err := signing.LoadSeedFromFile(signingKeyPath)
	if err != nil {
		return tokenParams{}, fmt.Errorf("loading signing key: %w", err)
	}

	return tokenParams{
		seed:       seed,
		keyID:      keyID,
		subject:    subject,
		scopes:     scopes,
		expiration: expiration,
	}, nil
}

func runGenerateToken(cmd *cobra.Command, _ []string) error {
	p, err := tokenParamsFromFlags(cmd)
	if err != nil {
		return err
	}

	token, err := signToken(p)
	if err != nil {
		return err
	}

	storeInKeychain, _ := cmd.Flags().GetBool("store")
	if storeInKeychain {
		server, _ := cmd.Flags().GetString("server")

		err := cmdutil.GetKeyring(cmd).Set(server, token)
		if err != nil {
			return fmt.Errorf("storing token in keychain: %w", err)
		}
		// Print confirmation to stderr to keep stdout clean for piping.
		_, _ = fmt.Fprintf(os.Stderr, "%s\n", pterm.Success.Sprintf("Token stored in keychain for server %s", pterm.Bold.Sprint(server)))
	}

	fmt.Print(token)

	return nil
}
