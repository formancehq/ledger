//go:build it

package test_suite

import (
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"time"

	"github.com/formancehq/go-libs/v3/oidc"
	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/go-jose/go-jose/v4"
	"github.com/go-jose/go-jose/v4/jwt"
)

const (
	// TestAudience is the audience used for test tokens
	TestAudience = "test-audience"
)

// GenerateTestJWT generates a JWT token signed with the test RSA key using go-libs/oidc
// This is used for integration tests to verify authentication
func GenerateTestJWT(claims *oidc.AccessTokenClaims) (string, error) {
	// Create RSA signer with test private key
	key := jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       GetTestPrivateKey(),
	}
	signer, err := jose.NewSigner(key, &jose.SignerOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to create signer: %w", err)
	}

	// Sign the token
	token, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return token, nil
}

// GenerateValidToken generates a valid JWT token with read and write scopes
func GenerateValidToken() (string, error) {
	now := libtime.Now()
	expiration := now.Add(time.Hour)

	claims := oidc.NewAccessTokenClaims(
		GetTestIssuer().GetValue(),
		"test-user",
		[]string{TestAudience},
		expiration,
		"test-jti",
		"test-client",
	)
	claims.Scopes = oidc.SpaceDelimitedArray{"ledger:read", "ledger:write"}

	return GenerateTestJWT(claims)
}

// GenerateInvalidToken generates an invalid JWT token (wrong signature)
func GenerateInvalidToken() (string, error) {
	now := libtime.Now()
	expiration := now.Add(time.Hour)

	claims := oidc.NewAccessTokenClaims(
		GetTestIssuer().GetValue(),
		"test-user",
		[]string{TestAudience},
		expiration,
		"test-jti",
		"test-client",
	)
	claims.Scopes = oidc.SpaceDelimitedArray{"ledger:read", "ledger:write"}

	// Generate a different RSA key to create an invalid signature
	wrongPrivateKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return "", fmt.Errorf("failed to generate wrong RSA key: %w", err)
	}

	// Create RSA signer with WRONG private key
	key := jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       wrongPrivateKey,
	}
	signer, err := jose.NewSigner(key, &jose.SignerOptions{})
	if err != nil {
		return "", fmt.Errorf("failed to create signer: %w", err)
	}

	// Sign the token with wrong key
	token, err := jwt.Signed(signer).Claims(claims).Serialize()
	if err != nil {
		return "", fmt.Errorf("failed to sign token: %w", err)
	}

	return token, nil
}

// GenerateExpiredToken generates an expired JWT token
func GenerateExpiredToken() (string, error) {
	now := libtime.Now()
	// Token expired 1 hour ago
	expiration := now.Add(-1 * time.Hour)
	issuedAt := now.Add(-2 * time.Hour)

	claims := oidc.NewAccessTokenClaims(
		GetTestIssuer().GetValue(),
		"test-user",
		[]string{TestAudience},
		expiration,
		"test-jti",
		"test-client",
	)
	claims.Scopes = oidc.SpaceDelimitedArray{"ledger:read", "ledger:write"}
	// IssuedAt is already set by NewAccessTokenClaims, but we override it for expired token
	// oidc.Time is an int64, so we create it directly from the Unix timestamp
	claims.IssuedAt = oidc.Time(issuedAt.Unix())

	return GenerateTestJWT(claims)
}
