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

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

const (
	mutationAuthContextKeyID   = "en-1950-key"
	mutationAuthContextSubject = "en-1950-user"
)

type mutationAuthContextAdmission struct {
	admit func(context.Context, *servicepb.ApplyRequest) ([]*commonpb.Log, error)
}

func (a *mutationAuthContextAdmission) Admit(ctx context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
	return a.admit(ctx, req)
}

func (a *mutationAuthContextAdmission) Barrier(context.Context) (uint64, error) {
	return 0, errors.New("unexpected Barrier call")
}

// TestMutationRPCsPropagateAuthenticatedContext guards every dedicated mutation
// RPC that enters Apply/Admit outside BucketService.Apply. Authenticate returns
// a derived context carrying the validated claims; dropping that return value
// makes ResolveCallerSnapshot nil and commits an unattributed audit entry.
func TestMutationRPCsPropagateAuthenticatedContext(t *testing.T) {
	t.Parallel()

	authCfg, ctx := mutationAuthContext(t)
	errCaptured := errors.New("mutation auth context captured")

	tests := []struct {
		name             string
		usesController   bool
		assertApplyBatch func(*testing.T, *servicepb.Request)
		invoke           func(context.Context, *BucketServiceServerImpl, *ClusterServiceServerImpl) error
	}{
		{
			name: "CreateQueryCheckpoint",
			assertApplyBatch: func(t *testing.T, req *servicepb.Request) {
				t.Helper()
				require.NotNil(t, req.GetCreateQueryCheckpoint())
			},
			invoke: func(ctx context.Context, _ *BucketServiceServerImpl, cluster *ClusterServiceServerImpl) error {
				_, err := cluster.CreateQueryCheckpoint(ctx, &clusterpb.CreateQueryCheckpointRequest{})

				return err
			},
		},
		{
			name: "DeleteQueryCheckpoint",
			assertApplyBatch: func(t *testing.T, req *servicepb.Request) {
				t.Helper()
				require.Equal(t, uint64(42), req.GetDeleteQueryCheckpoint().GetCheckpointId())
			},
			invoke: func(ctx context.Context, _ *BucketServiceServerImpl, cluster *ClusterServiceServerImpl) error {
				_, err := cluster.DeleteQueryCheckpoint(ctx, &clusterpb.DeleteQueryCheckpointRequest{CheckpointId: 42})

				return err
			},
		},
		{
			name:           "CreatePreparedQuery",
			usesController: true,
			assertApplyBatch: func(t *testing.T, req *servicepb.Request) {
				t.Helper()
				prepared := req.GetCreatePreparedQuery()
				require.Equal(t, "main", prepared.GetLedger())
				require.Equal(t, "accounts-by-owner", prepared.GetQuery().GetName())
			},
			invoke: func(ctx context.Context, bucket *BucketServiceServerImpl, _ *ClusterServiceServerImpl) error {
				_, err := bucket.CreatePreparedQuery(ctx, &servicepb.CreatePreparedQueryRequest{
					Ledger: "main",
					Query:  &commonpb.PreparedQuery{Name: "accounts-by-owner"},
				})

				return err
			},
		},
		{
			name:           "UpdatePreparedQuery",
			usesController: true,
			assertApplyBatch: func(t *testing.T, req *servicepb.Request) {
				t.Helper()
				prepared := req.GetUpdatePreparedQuery()
				require.Equal(t, "main", prepared.GetLedger())
				require.Equal(t, "accounts-by-owner", prepared.GetName())
			},
			invoke: func(ctx context.Context, bucket *BucketServiceServerImpl, _ *ClusterServiceServerImpl) error {
				_, err := bucket.UpdatePreparedQuery(ctx, &servicepb.UpdatePreparedQueryRequest{
					Ledger: "main",
					Name:   "accounts-by-owner",
				})

				return err
			},
		},
		{
			name:           "DeletePreparedQuery",
			usesController: true,
			assertApplyBatch: func(t *testing.T, req *servicepb.Request) {
				t.Helper()
				prepared := req.GetDeletePreparedQuery()
				require.Equal(t, "main", prepared.GetLedger())
				require.Equal(t, "accounts-by-owner", prepared.GetName())
			},
			invoke: func(ctx context.Context, bucket *BucketServiceServerImpl, _ *ClusterServiceServerImpl) error {
				_, err := bucket.DeletePreparedQuery(ctx, &servicepb.DeletePreparedQueryRequest{
					Ledger: "main",
					Name:   "accounts-by-owner",
				})

				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			var captured *commonpb.CallerSnapshot
			capture := func(ctx context.Context, req *servicepb.ApplyRequest) ([]*commonpb.Log, error) {
				requests := req.GetUnsigned().GetRequests()
				require.Len(t, requests, 1)
				test.assertApplyBatch(t, requests[0])
				captured = internalauth.ResolveCallerSnapshot(ctx)

				return nil, errCaptured
			}

			controller := NewMockController(gomock.NewController(t))
			if test.usesController {
				controller.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(capture)
			}
			admission := &mutationAuthContextAdmission{admit: capture}
			bucket := &BucketServiceServerImpl{ctrl: controller, authCfg: authCfg}
			cluster := &ClusterServiceServerImpl{admission: admission, authCfg: authCfg}

			err := test.invoke(ctx, bucket, cluster)

			require.ErrorIs(t, err, errCaptured)
			require.NotNil(t, captured, "the downstream write path must receive the authenticated context")
			require.Equal(t, mutationAuthContextSubject, captured.GetIdentity().GetSubject())
			require.Equal(t, mutationAuthContextKeyID, captured.GetIdentity().GetKeyId())
			require.Equal(t, []string{
				string(internalauth.ScopeClusterWrite),
				string(internalauth.ScopeQueriesWrite),
			}, captured.GetScopes())
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
