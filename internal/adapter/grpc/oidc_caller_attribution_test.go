package grpc

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source ../../application/admission/admission.go -destination oidc_proposer_generated_test.go -typed -package grpc

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	admissionapp "github.com/formancehq/ledger/v3/internal/application/admission"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/keystore"
	"github.com/formancehq/ledger/v3/internal/domain/processing/numscript"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/plan"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestOIDCCallerAttributionThroughPublicGRPC(t *testing.T) {
	t.Parallel()
	const issuer = "https://identity.example.com"
	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	rotatedKey, err := rsa.GenerateKey(rand.Reader, 2048)
	require.NoError(t, err)
	staticPublic, staticPrivate, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	cfg := internalauth.AuthConfig{
		Enabled: true, Issuers: []string{issuer}, Service: "ledger", ScopeMapping: internalauth.DefaultMapping("ledger"),
		KeySet: oidc.NewStaticKeySet(
			jose.JSONWebKey{Key: &rsaKey.PublicKey, KeyID: "oidc-first", Algorithm: string(jose.RS256), Use: "sig"},
			jose.JSONWebKey{Key: &rotatedKey.PublicKey, KeyID: "oidc-rotated", Algorithm: string(jose.RS256), Use: "sig"},
			jose.JSONWebKey{Key: staticPublic, KeyID: "static-key", Algorithm: string(jose.EdDSA), Use: "sig"},
		),
		Ed25519AllowedScopes: map[string][]string{"static-key": {"ledger:WriteLedger"}},
	}
	for _, tc := range []struct {
		name, keyID, tokenIssuer string
		algorithm                jose.SignatureAlgorithm
		key                      any
		wantSource               any
	}{
		{"OIDC with key ID", "oidc-first", issuer, jose.RS256, rsaKey, &commonpb.CallerIdentity_Issuer{Issuer: issuer}},
		{"rotated OIDC key", "oidc-rotated", issuer, jose.RS256, rotatedKey, &commonpb.CallerIdentity_Issuer{Issuer: issuer}},
		{"static key", "static-key", "", jose.EdDSA, staticPrivate, &commonpb.CallerIdentity_KeyId{KeyId: "static-key"}},
		{"static key with issuer claim", "static-key", issuer, jose.EdDSA, staticPrivate, &commonpb.CallerIdentity_KeyId{KeyId: "static-key"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			admission, proposals := newCallerAttributionAdmission(t, 1)
			server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, cfg)
			server.applyFn = admission.Admit
			token := callerAttributionToken(t, tc.algorithm, tc.keyID, tc.key, tc.tokenIssuer, "ledger:WriteLedger")
			ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token), 10*time.Second)
			defer cancel()
			_, err := client.Apply(ctx, callerAttributionWrite())
			require.NoError(t, err)
			snapshot := receiveCallerAttribution(t, ctx, proposals)
			authenticated := snapshot.GetAuthenticated()
			require.NotNil(t, authenticated)
			require.Equal(t, "alice", authenticated.GetIdentity().GetSubject())
			require.Equal(t, tc.wantSource, authenticated.GetIdentity().GetSource())
			require.Equal(t, []string{"ledger:WriteLedger"}, authenticated.GetScopes())
			require.False(t, authenticated.GetSuperuser())
			require.EqualValues(t, 1, server.applyCalls.Load())
		})
	}
	t.Run("insufficient scope never reaches admission", func(t *testing.T) {
		t.Parallel()
		admission, proposals := newCallerAttributionAdmission(t, 0)
		server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, cfg)
		server.applyFn = admission.Admit
		token := callerAttributionToken(t, jose.RS256, "oidc-first", rsaKey, issuer, "ledger:ReadLedger")
		ctx := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token)
		_, err := client.Apply(ctx, callerAttributionWrite())
		require.Equal(t, codes.PermissionDenied, status.Code(err))
		require.Zero(t, server.applyCalls.Load())
		require.Empty(t, proposals)
	})
	t.Run("trusted follower retains original issuer", func(t *testing.T) {
		t.Parallel()
		admission, proposals := newCallerAttributionAdmission(t, 1)
		leaderConfig := cfg
		leaderConfig.ClusterSecret = "trusted-cluster-secret"
		leader, leaderClient := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, leaderConfig)
		leader.applyFn = func(ctx context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
			ctx, err := (&BucketServiceServerImpl{}).adoptForwardedSnapshotIfTrusted(ctx, req)
			if err != nil {
				return nil, err
			}
			result, err := admission.Admit(ctx, req)
			if err != nil {
				return nil, err
			}
			if err := ggrpc.SetTrailer(ctx, metadata.Pairs(metadataKeyApplyReplayed, "false")); err != nil {
				return nil, err
			}

			return result, nil
		}
		follower, followerClient := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, cfg)
		forwarder := NewLedgerGrpcClient(leaderClient)
		follower.applyFn = func(ctx context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
			return forwarder.Apply(metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+leaderConfig.ClusterSecret), req)
		}
		token := callerAttributionToken(t, jose.RS256, "oidc-first", rsaKey, issuer, "ledger:WriteLedger")
		ctx, cancel := context.WithTimeout(metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer "+token), 10*time.Second)
		defer cancel()
		_, err := followerClient.Apply(ctx, callerAttributionWrite())
		require.NoError(t, err)
		identity := receiveCallerAttribution(t, ctx, proposals).GetAuthenticated().GetIdentity()
		require.Equal(t, "alice", identity.GetSubject())
		require.Equal(t, &commonpb.CallerIdentity_Issuer{Issuer: issuer}, identity.GetSource())
		require.EqualValues(t, 1, follower.applyCalls.Load())
		require.EqualValues(t, 1, leader.applyCalls.Load())
	})
}

func callerAttributionToken(t *testing.T, algorithm jose.SignatureAlgorithm, keyID string, key any, issuer, scope string) string {
	t.Helper()
	signer, err := jose.NewSigner(jose.SigningKey{Algorithm: algorithm, Key: &jose.JSONWebKey{Key: key, KeyID: keyID}}, nil)
	require.NoError(t, err)
	claims := &oidc.AccessTokenClaims{}
	claims.Subject, claims.Issuer = "alice", issuer
	claims.Expiration = oidc.FromTime(oidc.Time(time.Now().Add(time.Hour).Unix()).AsTime())
	claims.Scopes = oidc.SpaceDelimitedArray{scope}
	payload, err := json.Marshal(claims)
	require.NoError(t, err)
	signed, err := signer.Sign(payload)
	require.NoError(t, err)
	token, err := signed.CompactSerialize()
	require.NoError(t, err)

	return token
}

func callerAttributionWrite() *servicepb.ApplyRequest {
	return servicepb.UnsignedApplyRequest("", &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{Name: "main"}}})
}

func newCallerAttributionAdmission(t *testing.T, expectedProposals int) (*admissionapp.Admission, <-chan *commonpb.CallerSnapshot) {
	t.Helper()
	logger := logging.Testing()
	provider := noop.NewMeterProvider()
	store, err := dal.NewStore(t.TempDir(), logger, provider.Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	batch := store.OpenWriteSession()
	require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobAuditKey}, []byte("0123456789abcdef0123456789abcdef")))
	require.NoError(t, state.SaveClusterPolicy(batch, &commonpb.ClusterPolicy{
		Revision: 1, QueryCheckpointLimit: 10,
		MetadataMaxEntriesPerEntity: domain.DefaultMetadataMaxEntriesPerEntity,
		MetadataMaxKeyBytes:         domain.DefaultMetadataMaxKeyBytes,
		MetadataMaxValueBytes:       domain.DefaultMetadataMaxValueBytes,
		MetadataMaxEntityBytes:      domain.DefaultMetadataMaxEntityBytes,
		MetadataMaxCommandBytes:     domain.DefaultMetadataMaxCommandBytes,
	}))
	require.NoError(t, batch.Commit())
	testCache, err := cache.New(100, nil)
	require.NoError(t, err)
	attrs := attributes.New()
	proposals := make(chan *commonpb.CallerSnapshot, 1)
	proposer := NewMockProposer(gomock.NewController(t))
	proposer.EXPECT().Propose(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, p *node.Proposal) (*futures.Future[state.ApplyResult], error) {
		command := &raftcmdpb.Proposal{}
		if err := command.UnmarshalVT(p.Data()); err != nil {
			return nil, err
		}
		proposals <- command.GetCallerSnapshot()
		p.Resolve(nil, nil)
		result := futures.New[state.ApplyResult]()
		result.Resolve(state.ApplyResult{}, nil)

		return result, nil
	}).Times(expectedProposals)
	admission := admissionapp.NewAdmission(store, logger, proposer,
		plan.NewBuilder(node.NewIndexTracker(1), testCache, attrs, store, nil, logger, 0),
		provider, allowWriteGate{}, keystore.NewKeyStore(), state.NewSharedState(), attrs,
		numscript.NewNumscriptCache(0), func(context.Context) error { return nil })

	return admission, proposals
}

func receiveCallerAttribution(t *testing.T, ctx context.Context, proposals <-chan *commonpb.CallerSnapshot) *commonpb.CallerSnapshot {
	t.Helper()
	select {
	case snapshot := <-proposals:
		return snapshot
	case <-ctx.Done():
		t.Fatal("admission did not submit a proposal before request deadline")

		return nil
	}
}
