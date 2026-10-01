package admission

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/json"
	"errors"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	servicepb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/health"
)

func TestAdmitRejectsMissingAttributionBeforeDependencies(t *testing.T) {
	t.Parallel()

	// Every Admission dependency is intentionally nil. Reaching any write gate,
	// store, preload, or proposal path would panic and fail the test.
	_, err := (&Admission{}).Admit(context.Background(), &servicepb.ApplyRequest{})

	var invalid *domain.ErrInvalidCallerAttribution
	require.ErrorAs(t, err, &invalid)
}

func TestAdmitAcceptsCredentialDerivedPrincipalVariants(t *testing.T) {
	t.Parallel()

	gateErr := errors.New("reached write gate")
	tests := map[string]func(t *testing.T) context.Context{
		"auth disabled": func(t *testing.T) context.Context {
			ctx, err := internalauth.EvaluateGRPCCredentials(context.Background(), internalauth.AuthConfig{})
			require.NoError(t, err)

			return ctx
		},
		"anonymous": func(t *testing.T) context.Context {
			ctx, err := internalauth.EvaluateGRPCCredentials(context.Background(), internalauth.AuthConfig{
				Enabled:      true,
				ScopeMapping: internalauth.DefaultMapping("ledger"),
			})
			require.NoError(t, err)

			return ctx
		},
		"authenticated": func(t *testing.T) context.Context {
			const keyID = "admission-test-key"
			publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
			require.NoError(t, err)
			keySet := oidc.NewStaticKeySet(jose.JSONWebKey{
				Key: publicKey, KeyID: keyID, Algorithm: string(jose.EdDSA), Use: "sig",
			})
			claims := &oidc.AccessTokenClaims{}
			claims.Subject = "alice"
			claims.IssuedAt = oidc.FromTime(oidc.Time(time.Now().Unix()).AsTime())
			claims.Expiration = oidc.FromTime(oidc.Time(time.Now().Add(time.Hour).Unix()).AsTime())
			claims.Scopes = oidc.SpaceDelimitedArray{"ledger:read"}
			payload, err := json.Marshal(claims)
			require.NoError(t, err)
			signer, err := jose.NewSigner(jose.SigningKey{
				Algorithm: jose.EdDSA,
				Key:       &jose.JSONWebKey{Key: privateKey, KeyID: keyID},
			}, nil)
			require.NoError(t, err)
			signed, err := signer.Sign(payload)
			require.NoError(t, err)
			token, err := signed.CompactSerialize()
			require.NoError(t, err)
			ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("authorization", "Bearer "+token))
			ctx, err = internalauth.EvaluateGRPCCredentials(ctx, internalauth.AuthConfig{
				Enabled:      true,
				KeySet:       keySet,
				ScopeMapping: internalauth.DefaultMapping("ledger"),
				Ed25519AllowedScopes: map[string][]string{
					keyID: {"ledger:read"},
				},
			})
			require.NoError(t, err)

			return ctx
		},
	}

	for name, makeContext := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			writeGate := health.NewMockWriteGate(gomock.NewController(t))
			writeGate.EXPECT().CheckWritesAllowed().Return(gateErr)
			admission := &Admission{writeGate: writeGate}

			_, err := admission.Admit(makeContext(t), &servicepb.ApplyRequest{})
			require.ErrorIs(t, err, gateErr)
		})
	}
}
