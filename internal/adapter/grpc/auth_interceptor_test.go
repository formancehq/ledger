package grpc

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	jose "github.com/go-jose/go-jose/v4"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
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
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
)

type authInterceptorBucketServer struct {
	servicepb.UnimplementedBucketServiceServer

	barrierCalls     atomic.Int32
	barrierHasScope  atomic.Bool
	discoveryCalls   atomic.Int32
	listIndexesCalls atomic.Int32
	getLogCalls      atomic.Int32
	listLogsCalls    atomic.Int32
	applyCalls       atomic.Int32
	applyFn          func(context.Context, *servicepb.ApplyRequest) (*domain.ApplyResult, error)
}

func TestRejectDirectClusterSecret(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer peer",
		"x-ledger-peer-forward", "apply",
	))
	err := rejectDirectClusterSecret(ctx, internalauth.AuthConfig{ClusterSecret: "peer"})
	require.Equal(t, codes.Unauthenticated, status.Code(err))

	duplicate := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer cluster-secret",
		"authorization", "Bearer unrelated",
	))
	require.Equal(t, codes.Unauthenticated, status.Code(rejectDirectClusterSecret(duplicate, internalauth.AuthConfig{ClusterSecret: "cluster-secret"})))
}

func TestPeerCredentialUsesDedicatedTransportMetadata(t *testing.T) {
	t.Parallel()

	cfg := internalauth.AuthConfig{Enabled: true, ClusterSecret: "cluster-secret"}

	legacy := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer cluster-secret",
	))
	require.Equal(t, codes.Unauthenticated, status.Code(requirePeerCredential(legacy, cfg)))

	peer := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		transport.PeerAuthMetadataKey, "Bearer cluster-secret",
	))
	require.NoError(t, requirePeerCredential(peer, cfg))
}

func TestBearerTokenFromGRPCContextRejectsAmbiguousAuthorization(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"authorization", "Bearer first",
		"authorization", "Bearer second",
	))

	_, _, err := internalauth.BearerTokenFromGRPCContext(ctx)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
}

type authTestServerStream struct{ ctx context.Context }

func (s *authTestServerStream) SetHeader(metadata.MD) error  { return nil }
func (s *authTestServerStream) SendHeader(metadata.MD) error { return nil }
func (s *authTestServerStream) SetTrailer(metadata.MD)       {}
func (s *authTestServerStream) Context() context.Context     { return s.ctx }
func (s *authTestServerStream) SendMsg(any) error            { return nil }
func (s *authTestServerStream) RecvMsg(any) error            { return nil }

func TestAuthStreamInterceptorGuardsInfrastructureMethodsOnPeerListener(t *testing.T) {
	interceptor := authStreamInterceptor(internalauth.AuthConfig{Enabled: true, ClusterSecret: "cluster-secret"}, true)
	info := &ggrpc.StreamServerInfo{FullMethod: "/grpc.reflection.v1.ServerReflection/ServerReflectionInfo"}
	called := false
	handler := func(any, ggrpc.ServerStream) error {
		called = true
		return nil
	}

	err := interceptor(nil, &authTestServerStream{ctx: context.Background()}, info, handler)
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.False(t, called)

	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(transport.PeerAuthMetadataKey, "Bearer cluster-secret"))
	err = interceptor(nil, &authTestServerStream{ctx: ctx}, info, handler)
	require.NoError(t, err)
	require.True(t, called)
}

func (s *authInterceptorBucketServer) Barrier(ctx context.Context, _ *servicepb.BarrierRequest) (*servicepb.BarrierResponse, error) {
	s.barrierCalls.Add(1)
	s.barrierHasScope.Store(internalauth.HasScope(internalauth.ExpandedScopesFromContext(ctx), internalauth.ScopeOpsRead))

	return &servicepb.BarrierResponse{}, nil
}

func (s *authInterceptorBucketServer) Discovery(context.Context, *servicepb.DiscoveryRequest) (*servicepb.DiscoveryResponse, error) {
	s.discoveryCalls.Add(1)

	return &servicepb.DiscoveryResponse{}, nil
}

func (s *authInterceptorBucketServer) ListIndexes(*servicepb.ListIndexesRequest, servicepb.BucketService_ListIndexesServer) error {
	s.listIndexesCalls.Add(1)

	return nil
}

func (s *authInterceptorBucketServer) GetLog(context.Context, *servicepb.GetLogRequest) (*commonpb.Log, error) {
	s.getLogCalls.Add(1)

	return &commonpb.Log{}, nil
}

func (s *authInterceptorBucketServer) ListLogs(*servicepb.ListLogsRequest, servicepb.BucketService_ListLogsServer) error {
	s.listLogsCalls.Add(1)

	return nil
}

func (s *authInterceptorBucketServer) Apply(ctx context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	s.applyCalls.Add(1)
	if s.applyFn != nil {
		if _, err := s.applyFn(ctx, req); err != nil {
			return nil, err
		}
	}

	return &servicepb.ApplyResponse{}, nil
}

func TestAuthInterceptorsEnforcePoliciesBeforeGeneratedHandlers(t *testing.T) {
	t.Parallel()

	cfg := internalauth.AuthConfig{
		Enabled: true,
		ScopeMapping: internalauth.ScopeMapping{
			internalauth.ScopeMappingAnonymousKey: {internalauth.ScopeLedgersRead},
		},
	}
	server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, cfg)

	_, err := client.Barrier(context.Background(), &servicepb.BarrierRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Zero(t, server.barrierCalls.Load(), "fixed-scope denial must precede handler invocation")

	stream, err := client.ListIndexes(context.Background(), &servicepb.ListIndexesRequest{
		Scope:  servicepb.ListIndexesRequest_SCOPE_LEDGER,
		Ledger: "main",
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	require.EqualValues(t, 1, server.listIndexesCalls.Load())

	stream, err = client.ListIndexes(context.Background(), &servicepb.ListIndexesRequest{
		Scope: servicepb.ListIndexesRequest_SCOPE_ALL,
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.EqualValues(t, 1, server.listIndexesCalls.Load(), "first-message denial must precede handler invocation")

	invalidToken := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer invalid")
	_, err = client.Discovery(invalidToken, &servicepb.DiscoveryRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 1, server.discoveryCalls.Load(), "public methods must bypass credential evaluation")
}

func TestAuthInterceptorAllowsFixedScopeAndEnrichesContext(t *testing.T) {
	t.Parallel()

	cfg := internalauth.AuthConfig{
		Enabled: true,
		ScopeMapping: internalauth.ScopeMapping{
			internalauth.ScopeMappingAnonymousKey: {internalauth.ScopeOpsRead},
		},
	}
	server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, cfg)

	_, err := client.Barrier(context.Background(), &servicepb.BarrierRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 1, server.barrierCalls.Load())
	require.True(t, server.barrierHasScope.Load())
}

func TestQueryProfileClockIncludesInterceptorWork(t *testing.T) {
	t.Parallel()

	const interceptorDelay = time.Hour
	var profile *query.QueryProfile

	clock := queryProfileClockUnaryInterceptorAt(noopLogger{}, time.Second, func() time.Time {
		return time.Now().Add(-interceptorDelay)
	})
	_, err := clock(context.Background(), nil, &ggrpc.UnaryServerInfo{}, func(ctx context.Context, _ any) (any, error) {
		_, profile = withTransportQueryProfile(ctx)
		profile.Finish()

		return nil, nil
	})
	require.NoError(t, err)
	require.NotNil(t, profile)
	require.GreaterOrEqual(t, profile.ServerDuration, interceptorDelay)
}

func TestProfiledRPCAuthDenialsEmitRequestedProfileThroughServerChain(t *testing.T) {
	t.Parallel()

	_, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, anonymousAuthConfig())
	ctx := metadata.NewOutgoingContext(context.Background(), metadata.Pairs(metadataKeyQueryProfile, "true"))

	t.Run("unary", func(t *testing.T) {
		var trailer metadata.MD
		_, err := client.AggregateVolumes(ctx, &servicepb.AggregateVolumesRequest{}, ggrpc.Trailer(&trailer))
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.NotEmpty(t, trailer.Get(metadataKeyQueryProfileResult))
	})

	t.Run("unprofiled unary", func(t *testing.T) {
		var trailer metadata.MD
		_, err := client.Barrier(ctx, &servicepb.BarrierRequest{}, ggrpc.Trailer(&trailer))
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.NotEmpty(t, trailer.Get(metadataKeyQueryProfileResult))
	})

	t.Run("stream", func(t *testing.T) {
		stream, err := client.ListTransactions(ctx, &servicepb.ListTransactionsRequest{})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.NotEmpty(t, stream.Trailer().Get(metadataKeyQueryProfileResult))
	})
}

func newAuthInterceptorServer(t *testing.T, mode ServiceAuthPolicy, cfg internalauth.AuthConfig) (*authInterceptorBucketServer, servicepb.BucketServiceClient) {
	t.Helper()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server, err := NewServiceServer(mode, cfg, "", 0, noopLogger{}, false, time.Second, nil, true, WithListener(listener))
	require.NoError(t, err)
	implementation := &authInterceptorBucketServer{}
	servicepb.RegisterBucketServiceServer(server.GetServer(), implementation)
	if mode == ServiceAuthPolicyPublic {
		clusterpb.RegisterClusterServiceServer(server.GetServer(), &clusterpb.UnimplementedClusterServiceServer{})
	}
	require.NoError(t, server.Listen())
	go func() { _ = server.Serve() }()
	t.Cleanup(func() { require.NoError(t, server.Stop()) })

	conn, err := ggrpc.NewClient(listener.Addr().String(),
		ggrpc.WithTransportCredentials(insecure.NewCredentials()),
		grpcprotocol.ClientOption(),
	)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })

	return implementation, servicepb.NewBucketServiceClient(conn)
}

func TestRestoreServiceDoesNotInstallPublicJWTAuthorization(t *testing.T) {
	t.Parallel()

	server, client := newAuthInterceptorServer(t, ServiceAuthPolicyRestore, internalauth.AuthConfig{Enabled: true})
	_, err := client.Barrier(context.Background(), &servicepb.BarrierRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 1, server.barrierCalls.Load())
}

func TestFixedPolicyCredentialModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cfg      internalauth.AuthConfig
		ctx      context.Context
		wantCode codes.Code
	}{
		{
			name:     "missing credentials",
			cfg:      anonymousAuthConfig(),
			ctx:      context.Background(),
			wantCode: codes.Unauthenticated,
		},
		{
			name:     "wrong anonymous scope",
			cfg:      anonymousAuthConfig(internalauth.ScopeAccountsRead),
			ctx:      context.Background(),
			wantCode: codes.Unauthenticated,
		},
		{
			name:     "required anonymous scope",
			cfg:      anonymousAuthConfig(internalauth.ScopeOpsRead),
			ctx:      context.Background(),
			wantCode: codes.OK,
		},
		{
			name:     "authentication disabled",
			cfg:      internalauth.AuthConfig{},
			ctx:      context.Background(),
			wantCode: codes.OK,
		},
		{
			name: "cluster secret is rejected on direct service API",
			cfg:  internalauth.AuthConfig{Enabled: true, ClusterSecret: "cluster-secret"},
			ctx: metadata.AppendToOutgoingContext(
				context.Background(), "authorization", "Bearer cluster-secret",
			),
			wantCode: codes.Unauthenticated,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, test.cfg)
			_, err := client.Barrier(test.ctx, &servicepb.BarrierRequest{})
			require.Equal(t, test.wantCode, status.Code(err))
			if test.wantCode == codes.OK {
				require.EqualValues(t, 1, server.barrierCalls.Load())
			} else {
				require.Zero(t, server.barrierCalls.Load())
			}
		})
	}
}

func TestFixedPolicyValidTokenWithoutScopeReturnsPermissionDenied(t *testing.T) {
	t.Parallel()

	cfg, incoming := mutationAuthContext(t)
	md, ok := metadata.FromIncomingContext(incoming)
	require.True(t, ok)
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, cfg)

	_, err := client.Barrier(ctx, &servicepb.BarrierRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Zero(t, server.barrierCalls.Load())
}

func TestListLogsAndGetLogKeepDistinctFixedScopes(t *testing.T) {
	t.Parallel()

	t.Run("ledger read lists logs but cannot fetch a bucket log", func(t *testing.T) {
		t.Parallel()

		server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, anonymousAuthConfig(internalauth.ScopeLedgersRead))
		stream, err := client.ListLogs(context.Background(), &servicepb.ListLogsRequest{})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.ErrorIs(t, err, io.EOF)
		require.EqualValues(t, 1, server.listLogsCalls.Load())

		_, err = client.GetLog(context.Background(), &servicepb.GetLogRequest{})
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.Zero(t, server.getLogCalls.Load())
	})

	t.Run("ops read fetches a bucket log but cannot list ledger logs", func(t *testing.T) {
		t.Parallel()

		server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, anonymousAuthConfig(internalauth.ScopeOpsRead))
		_, err := client.GetLog(context.Background(), &servicepb.GetLogRequest{})
		require.NoError(t, err)
		require.EqualValues(t, 1, server.getLogCalls.Load())

		stream, err := client.ListLogs(context.Background(), &servicepb.ListLogsRequest{})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.Zero(t, server.listLogsCalls.Load())
	})
}

func TestApplyDynamicPolicyUsesEveryEmbeddedBusinessScope(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		request   *servicepb.Request
		granted   internalauth.Scope
		wantCode  codes.Code
		wantScope internalauth.Scope
	}{
		{"numscript save with LedgerWrite", &servicepb.Request{Type: &servicepb.Request_SaveNumscript{SaveNumscript: &servicepb.SaveNumscriptRequest{}}}, internalauth.ScopeLedgersWrite, codes.OK, ""},
		{"numscript save with OpsWrite", &servicepb.Request{Type: &servicepb.Request_SaveNumscript{SaveNumscript: &servicepb.SaveNumscriptRequest{}}}, internalauth.ScopeOpsWrite, codes.Unauthenticated, internalauth.ScopeLedgersWrite},
		{"add account type with MetadataWrite", &servicepb.Request{Type: &servicepb.Request_AddAccountType{AddAccountType: &servicepb.AddAccountTypeLedgerRequest{}}}, internalauth.ScopeMetadataWrite, codes.OK, ""},
		{"add account type with OpsWrite", &servicepb.Request{Type: &servicepb.Request_AddAccountType{AddAccountType: &servicepb.AddAccountTypeLedgerRequest{}}}, internalauth.ScopeOpsWrite, codes.Unauthenticated, internalauth.ScopeMetadataWrite},
		{"save ledger metadata with MetadataWrite", &servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{}}}, internalauth.ScopeMetadataWrite, codes.OK, ""},
		{"checkpoint with ClusterWrite", &servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}}, internalauth.ScopeClusterWrite, codes.OK, ""},
		{"checkpoint with OpsWrite", &servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}}, internalauth.ScopeOpsWrite, codes.Unauthenticated, internalauth.ScopeClusterWrite},
		{"delete checkpoint with OpsWrite", &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{}}}, internalauth.ScopeOpsWrite, codes.Unauthenticated, internalauth.ScopeClusterWrite},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, anonymousAuthConfig(test.granted))
			_, err := client.Apply(context.Background(), servicepb.UnsignedApplyRequest("", test.request))
			require.Equal(t, test.wantCode, status.Code(err))
			if test.wantCode == codes.OK {
				require.EqualValues(t, 1, server.applyCalls.Load())

				return
			}
			require.Zero(t, server.applyCalls.Load())
			require.Contains(t, status.Convert(err).Message(), "requires scope "+string(test.wantScope))
		})
	}
}

func TestApplyDynamicPolicyAuthorizesEveryEmbeddedRequest(t *testing.T) {
	t.Parallel()

	server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, anonymousAuthConfig(internalauth.ScopeMetadataWrite))
	_, err := client.Apply(context.Background(), servicepb.UnsignedApplyRequest("",
		&servicepb.Request{Type: &servicepb.Request_AddAccountType{
			AddAccountType: &servicepb.AddAccountTypeLedgerRequest{},
		}},
		&servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{
			CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{},
		}},
	))
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "request 1 requires scope "+string(internalauth.ScopeClusterWrite))
	require.Zero(t, server.applyCalls.Load())
}

func TestDistinctApplyScopesPreservesFirstRequestOrder(t *testing.T) {
	t.Parallel()

	requests := []*servicepb.Request{
		{Type: &servicepb.Request_AddAccountType{AddAccountType: &servicepb.AddAccountTypeLedgerRequest{}}},
		{Type: &servicepb.Request_AddAccountType{AddAccountType: &servicepb.AddAccountTypeLedgerRequest{}}},
		{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}},
	}

	require.Equal(t, []indexedScope{
		{scope: internalauth.ScopeMetadataWrite, index: 0},
		{scope: internalauth.ScopeClusterWrite, index: 2},
	}, distinctApplyScopes(requests))
}

func anonymousAuthConfig(scopes ...internalauth.Scope) internalauth.AuthConfig {
	return internalauth.AuthConfig{
		Enabled: true,
		ScopeMapping: internalauth.ScopeMapping{
			internalauth.ScopeMappingAnonymousKey: scopes,
		},
	}
}

func TestApplyDynamicAuthorizationPreservesSignedPayloadPrecedence(t *testing.T) {
	t.Parallel()

	ctx, err := internalauth.EvaluateGRPCCredentials(context.Background(), internalauth.AuthConfig{
		Enabled: true,
		ScopeMapping: internalauth.ScopeMapping{
			internalauth.ScopeMappingAnonymousKey: {internalauth.ScopeMetadataWrite},
		},
	})
	require.NoError(t, err)

	tests := []struct {
		name     string
		request  *servicepb.ApplyRequest
		wantCode codes.Code
	}{
		{
			name: "authorized request",
			request: servicepb.UnsignedApplyRequest("", &servicepb.Request{
				Type: &servicepb.Request_AddAccountType{},
			}),
			wantCode: codes.OK,
		},
		{
			name: "unauthorized embedded request",
			request: servicepb.UnsignedApplyRequest("", &servicepb.Request{
				Type: &servicepb.Request_CreateQueryCheckpoint{},
			}),
			wantCode: codes.Unauthenticated,
		},
		{
			name:     "malformed unsigned payload",
			request:  &servicepb.ApplyRequest{},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "malformed signed payload reaches signature admission",
			request: servicepb.SignedApplyRequest(&signaturepb.SignedApplyBatch{
				Payload: []byte{0xff},
			}),
			wantCode: codes.OK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			authorizedCtx, err := authorizeDynamicUnaryRPC(ctx, test.request, commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_APPLY)
			require.Equal(t, test.wantCode, status.Code(err))
			if test.name == "authorized request" {
				require.Equal(t, 1, authorizedCtx.Value(applyBatchSizeKey{}))
			}
		})
	}
}

func TestMalformedSignedApplyReachesAdmissionSignatureVerification(t *testing.T) {
	t.Parallel()

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)
	admission := newSignatureAdmission(t, "test-key", publicKey)
	server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, internalauth.AuthConfig{})
	server.applyFn = admission.Admit

	invalidSignature := servicepb.SignedApplyRequest(&signaturepb.SignedApplyBatch{
		KeyId:     "test-key",
		Payload:   []byte{0xff},
		Signature: make([]byte, ed25519.SignatureSize),
	})
	_, err = client.Apply(context.Background(), invalidSignature)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	malformedPayload := []byte{0xff}
	validSignature := servicepb.SignedApplyRequest(&signaturepb.SignedApplyBatch{
		KeyId:     "test-key",
		Payload:   malformedPayload,
		Signature: ed25519.Sign(privateKey, malformedPayload),
	})
	_, err = client.Apply(context.Background(), validSignature)
	require.Equal(t, codes.Unknown, status.Code(err), "a valid signature must reach admission payload extraction")
	require.EqualValues(t, 2, server.applyCalls.Load())
}

type allowWriteGate struct{}

func (allowWriteGate) CheckWritesAllowed() error { return nil }

func newSignatureAdmission(t *testing.T, keyID string, publicKey ed25519.PublicKey) *admissionapp.Admission {
	t.Helper()

	logger := logging.Testing()
	meterProvider := noop.NewMeterProvider()
	store, err := dal.NewStore(t.TempDir(), logger, meterProvider.Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	cache, err := cache.New(100, nil)
	require.NoError(t, err)
	attrs := attributes.New()
	keys := keystore.NewKeyStore()
	keys.AddPublicKey(keyID, publicKey, "")

	return admissionapp.NewAdmission(
		store,
		logger,
		nil,
		plan.NewBuilder(node.NewIndexTracker(1), cache, attrs, store, nil, logger, 0),
		meterProvider,
		allowWriteGate{},
		keys,
		state.NewSharedState(),
		attrs,
		numscript.NewNumscriptCache(0),
		func(context.Context) error { return nil },
	)
}

func TestAuthenticationStateIsIndependentFromLegacyScopeContext(t *testing.T) {
	t.Parallel()

	ctx, err := internalauth.EvaluateGRPCCredentials(context.Background(), internalauth.AuthConfig{
		Enabled: true,
		ScopeMapping: internalauth.ScopeMapping{
			internalauth.ScopeMappingAnonymousKey: {internalauth.ScopeLedgersRead},
		},
	})
	require.NoError(t, err)

	delete(internalauth.ExpandedScopesFromContext(ctx), internalauth.ScopeLedgersRead)
	require.NoError(t, internalauth.AuthorizeGRPC(ctx, internalauth.ScopeLedgersRead))
}

func TestAuthScopeMapsEveryDeclaredScope(t *testing.T) {
	t.Parallel()

	mapped := map[internalauth.Scope]struct{}{}
	for number, name := range commonpb.AuthScope_name {
		scope := commonpb.AuthScope(number)
		if scope == commonpb.AuthScope_AUTH_SCOPE_UNSPECIFIED {
			continue
		}

		internal, err := authScope(scope)
		require.NoError(t, err, name)
		mapped[internal] = struct{}{}
	}

	require.Equal(t, internalauth.AllGranularScopes, mapped)
	_, err := authScope(commonpb.AuthScope(999))
	require.Equal(t, codes.Internal, status.Code(err))
}

type authInterceptorClusterServer struct {
	clusterpb.UnimplementedClusterServiceServer

	transferLeadershipCalls atomic.Int32
}

func (s *authInterceptorClusterServer) TransferLeadership(context.Context, *clusterpb.TransferLeadershipRequest) (*clusterpb.TransferLeadershipResponse, error) {
	s.transferLeadershipCalls.Add(1)

	return &clusterpb.TransferLeadershipResponse{}, nil
}

func TestAuthInterceptorLoadedEd25519Privileges(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	privateKeys := make(map[string]ed25519.PrivateKey)
	entries := make([]internalauth.Ed25519KeyEntry, 0, 2)
	for _, id := range []string{"restricted", "privileged"} {
		publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
		require.NoError(t, err)
		privateKeys[id] = privateKey
		path := filepath.Join(dir, id+".hex")
		require.NoError(t, os.WriteFile(path, []byte(hex.EncodeToString(publicKey)), 0600))
		entry := internalauth.Ed25519KeyEntry{KeyID: id, PublicKeyFile: path, Scopes: []string{"ledger:read"}}
		if id == "privileged" {
			entry.Scopes = []string{"ledger:admin"}
			entry.Superuser = true
		}
		entries = append(entries, entry)
	}
	data, err := json.Marshal(internalauth.Ed25519KeysConfig{Keys: entries})
	require.NoError(t, err)
	path := filepath.Join(dir, "keys.json")
	require.NoError(t, os.WriteFile(path, data, 0600))
	loaded, err := internalauth.LoadEd25519KeySet(path)
	require.NoError(t, err)
	cfg := internalauth.AuthConfig{
		Enabled: true, KeySet: loaded.KeySet, Service: "ledger", ScopeMapping: internalauth.DefaultMapping("ledger"),
		Ed25519AllowedScopes: loaded.AllowedScopes, Ed25519SuperuserKeys: loaded.SuperuserKeys,
	}
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server, err := NewServiceServer(ServiceAuthPolicyPublic, cfg, "", 0, noopLogger{}, false, time.Second, nil, true, WithListener(listener))
	require.NoError(t, err)
	implementation := &authInterceptorClusterServer{}
	servicepb.RegisterBucketServiceServer(server.GetServer(), &servicepb.UnimplementedBucketServiceServer{})
	clusterpb.RegisterClusterServiceServer(server.GetServer(), implementation)
	require.NoError(t, server.Listen())
	go func() {
		// Stop closes the listener; shutdown errors are covered by the cleanup below.
		_ = server.Serve()
	}()
	t.Cleanup(func() { require.NoError(t, server.Stop()) })
	conn, err := ggrpc.NewClient(listener.Addr().String(), ggrpc.WithTransportCredentials(insecure.NewCredentials()), grpcprotocol.ClientOption())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, conn.Close()) })
	client := clusterpb.NewClusterServiceClient(conn)

	for _, test := range []struct {
		name      string
		keyID     string
		scopes    []string
		superuser bool
		code      codes.Code
	}{
		{name: "restricted superuser", keyID: "restricted", superuser: true, code: codes.Unauthenticated},
		{name: "restricted excessive scope", keyID: "restricted", scopes: []string{"ledger:admin"}, code: codes.Unauthenticated},
		{name: "restricted valid read", keyID: "restricted", scopes: []string{"ledger:read"}, code: codes.PermissionDenied},
		{name: "privileged admin scope", keyID: "privileged", scopes: []string{"ledger:admin"}},
		{name: "privileged superuser", keyID: "privileged", superuser: true},
	} {
		// Cases share a backend counter so each RPC can prove its exact entry count.
		t.Run(test.name, func(t *testing.T) {
			claims := &oidc.AccessTokenClaims{}
			claims.Subject = test.keyID
			claims.Expiration = oidc.Time(time.Now().Add(time.Hour).Unix())
			claims.Scopes = oidc.SpaceDelimitedArray(test.scopes)
			claims.Claims = map[string]any{"superuser": test.superuser}
			payload, err := json.Marshal(claims)
			require.NoError(t, err)
			signer, err := jose.NewSigner(jose.SigningKey{Algorithm: jose.EdDSA, Key: &jose.JSONWebKey{Key: privateKeys[test.keyID], KeyID: test.keyID}}, nil)
			require.NoError(t, err)
			signed, err := signer.Sign(payload)
			require.NoError(t, err)
			token, err := signed.CompactSerialize()
			require.NoError(t, err)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
			before := implementation.transferLeadershipCalls.Load()
			_, err = client.TransferLeadership(ctx, &clusterpb.TransferLeadershipRequest{})
			require.Equal(t, test.code, status.Code(err))
			expected := before
			if test.code == codes.OK {
				expected++
			}
			require.Equal(t, expected, implementation.transferLeadershipCalls.Load(), "authorization must precede backend entry")
		})
	}
	require.EqualValues(t, 2, implementation.transferLeadershipCalls.Load())
}
