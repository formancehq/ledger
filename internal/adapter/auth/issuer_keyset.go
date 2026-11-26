package auth

import (
	"context"
	"encoding/json"
	"fmt"

	jose "github.com/go-jose/go-jose/v4"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
)

// issuerKeySet verifies a token only with the key set of the issuer its
// unverified "iss" claim names, so one trusted issuer's keys can never vouch
// for a token claiming another issuer. validateToken still checks the issuer
// once the signature is verified.
type issuerKeySet struct {
	keySets map[string]oidc.KeySet
}

// NewIssuerKeySet creates a KeySet that routes verification by issuer.
func NewIssuerKeySet(keySets map[string]oidc.KeySet) oidc.KeySet {
	return &issuerKeySet{keySets: keySets}
}

func (s *issuerKeySet) VerifySignature(ctx context.Context, jws *jose.JSONWebSignature) ([]byte, error) {
	var claims struct {
		Issuer string `json:"iss"`
	}
	if err := json.Unmarshal(jws.UnsafePayloadWithoutVerification(), &claims); err != nil {
		return nil, fmt.Errorf("decoding token issuer: %w", err)
	}

	keySet, ok := s.keySets[claims.Issuer]
	if !ok {
		return nil, fmt.Errorf("%w: untrusted issuer %q", oidc.ErrIssuerInvalid, claims.Issuer)
	}

	return keySet.VerifySignature(ctx, jws)
}
