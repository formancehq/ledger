package auth

import (
	"context"

	jose "github.com/go-jose/go-jose/v4"

	"github.com/formancehq/go-libs/v4/oidc"
)

// compositeKeySet tries multiple KeySets in order until one succeeds.
// This allows routing JWT verification across multiple key sources (e.g. OIDC + Ed25519 static keys).
type compositeKeySet struct {
	keySets []oidc.KeySet
}

// NewCompositeKeySet creates a KeySet that tries the given key sets in order.
// Nil entries are filtered out. If only one key set remains, it is returned directly.
func NewCompositeKeySet(keySets ...oidc.KeySet) oidc.KeySet {
	var filtered []oidc.KeySet

	for _, ks := range keySets {
		if ks != nil {
			filtered = append(filtered, ks)
		}
	}

	switch len(filtered) {
	case 0:
		return nil
	case 1:
		return filtered[0]
	default:
		return &compositeKeySet{keySets: filtered}
	}
}

func (c *compositeKeySet) VerifySignature(ctx context.Context, jws *jose.JSONWebSignature) ([]byte, error) {
	var lastErr error

	for _, ks := range c.keySets {
		payload, err := ks.VerifySignature(ctx, jws)
		if err == nil {
			return payload, nil
		}

		lastErr = err
	}

	if lastErr != nil {
		return nil, lastErr
	}

	return nil, oidc.ErrKeyNone
}
