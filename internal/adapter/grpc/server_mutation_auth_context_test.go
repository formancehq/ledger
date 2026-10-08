package grpc

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
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
)

const (
	mutationAuthContextKeyID   = "en-1950-key"
	mutationAuthContextSubject = "en-1950-user"
)

// TestMutationRequestsPropagateAuthenticatedContext guards every audited write
// that used to have a dedicated mutation RPC entering Apply/Admit outside
// BucketService.Apply (EN-1950). EN-1954 removed those five RPCs; the same
// defect is now only reachable through Apply, so the trigger moves here rather
// than being deleted. Authenticate returns a derived context carrying the
// validated claims; dropping that return value makes ResolveCallerSnapshot nil
// and commits an unattributed audit entry.
func TestMutationRequestsPropagateAuthenticatedContext(t *testing.T) {
	t.Parallel()

	authCfg, ctx := mutationAuthContext(t)
	errCaptured := errors.New("mutation auth context captured")

	tests := []struct {
		name        string
		request     *ledgerpb.Request
		assertBatch func(*testing.T, *ledgerpb.Request)
	}{
		{
			name: "CreateQueryCheckpoint",
			request: &ledgerpb.Request{
				Type: &ledgerpb.Request_CreateQueryCheckpoint{
					CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{},
				},
			},
			assertBatch: func(t *testing.T, req *ledgerpb.Request) {
				t.Helper()
				require.NotNil(t, req.GetCreateQueryCheckpoint())
			},
		},
		{
			name: "DeleteQueryCheckpoint",
			request: &ledgerpb.Request{
				Type: &ledgerpb.Request_DeleteQueryCheckpoint{
					DeleteQueryCheckpoint: &ledgerpb.DeleteQueryCheckpointRequest{CheckpointId: 42},
				},
			},
			assertBatch: func(t *testing.T, req *ledgerpb.Request) {
				t.Helper()
				require.Equal(t, uint64(42), req.GetDeleteQueryCheckpoint().GetCheckpointId())
			},
		},
		{
			name: "CreatePreparedQuery",
			request: &ledgerpb.Request{
				Type: &ledgerpb.Request_CreatePreparedQuery{
					CreatePreparedQuery: &ledgerpb.CreatePreparedQueryRequest{
						Ledger: "main",
						Query:  &ledgerpb.PreparedQuery{Name: "accounts-by-owner"},
					},
				},
			},
			assertBatch: func(t *testing.T, req *ledgerpb.Request) {
				t.Helper()
				prepared := req.GetCreatePreparedQuery()
				require.Equal(t, "main", prepared.GetLedger())
				require.Equal(t, "accounts-by-owner", prepared.GetQuery().GetName())
			},
		},
		{
			name: "UpdatePreparedQuery",
			request: &ledgerpb.Request{
				Type: &ledgerpb.Request_UpdatePreparedQuery{
					UpdatePreparedQuery: &ledgerpb.UpdatePreparedQueryRequest{
						Ledger: "main",
						Name:   "accounts-by-owner",
					},
				},
			},
			assertBatch: func(t *testing.T, req *ledgerpb.Request) {
				t.Helper()
				prepared := req.GetUpdatePreparedQuery()
				require.Equal(t, "main", prepared.GetLedger())
				require.Equal(t, "accounts-by-owner", prepared.GetName())
			},
		},
		{
			name: "DeletePreparedQuery",
			request: &ledgerpb.Request{
				Type: &ledgerpb.Request_DeletePreparedQuery{
					DeletePreparedQuery: &ledgerpb.DeletePreparedQueryRequest{
						Ledger: "main",
						Name:   "accounts-by-owner",
					},
				},
			},
			assertBatch: func(t *testing.T, req *ledgerpb.Request) {
				t.Helper()
				prepared := req.GetDeletePreparedQuery()
				require.Equal(t, "main", prepared.GetLedger())
				require.Equal(t, "accounts-by-owner", prepared.GetName())
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var captured *ledgerpb.CallerSnapshot
			controller := NewMockController(gomock.NewController(t))
			controller.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
				func(ctx context.Context, req *ledgerpb.ApplyRequest) (*domain.ApplyResult, error) {
					requests := req.GetUnsigned().GetRequests()
					require.Len(t, requests, 1)
					test.assertBatch(t, requests[0])
					captured = internalauth.ResolveCallerSnapshot(ctx)

					return nil, errCaptured
				})

			ctx, err := internalauth.EvaluateGRPCCredentials(ctx, authCfg)
			require.NoError(t, err)

			bucket := &BucketServiceServerImpl{
				logger: logging.Testing(),
				ctrl:   controller,
			}

			_, err = bucket.Apply(ctx, ledgerpb.UnsignedApplyRequest("", test.request))

			require.ErrorIs(t, err, errCaptured)
			require.NotNil(t, captured, "the downstream write path must receive the authenticated context")
			require.Equal(t, mutationAuthContextSubject, captured.GetAuthenticated().GetIdentity().GetSubject())
			require.Equal(t, mutationAuthContextKeyID, captured.GetAuthenticated().GetIdentity().GetKeyId())
			require.Equal(t, []string{
				string(internalauth.ScopeClusterWrite),
				string(internalauth.ScopeQueriesWrite),
			}, captured.GetAuthenticated().GetScopes())
		})
	}
}

func mutationAuthContext(t *testing.T) (internalauth.AuthConfig, context.Context) {
	t.Helper()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	keySet := oidc.NewStaticKeySet(jose.JSONWebKey{
		Key:       publicKey,
		KeyID:     mutationAuthContextKeyID,
		Algorithm: string(jose.EdDSA),
		Use:       "sig",
	})
	authCfg := internalauth.AuthConfig{
		Enabled:      true,
		KeySet:       keySet,
		ScopeMapping: internalauth.DefaultMapping("ledger"),
	}

	now := time.Now()
	claims := &oidc.AccessTokenClaims{}
	claims.Subject = mutationAuthContextSubject
	claims.IssuedAt = oidc.FromTime(oidc.Time(now.Unix()).AsTime())
	claims.Expiration = oidc.FromTime(oidc.Time(now.Add(time.Hour).Unix()).AsTime())
	claims.Scopes = oidc.SpaceDelimitedArray{
		string(internalauth.ScopeClusterWrite),
		string(internalauth.ScopeQueriesWrite),
	}
	payload, err := json.Marshal(claims)
	require.NoError(t, err)

	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.EdDSA,
		Key:       &jose.JSONWebKey{Key: privateKey, KeyID: mutationAuthContextKeyID},
	}, nil)
	require.NoError(t, err)

	signed, err := signer.Sign(payload)
	require.NoError(t, err)
	token, err := signed.CompactSerialize()
	require.NoError(t, err)

	ctx := metadata.NewIncomingContext(
		context.Background(),
		metadata.Pairs("authorization", "Bearer "+token),
	)

	return authCfg, ctx
}
