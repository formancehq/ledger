package grpc

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

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
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
)

type authInterceptorBucketServer struct {
	ledgerpb.UnimplementedBucketServiceServer

	barrierCalls     atomic.Int32
	barrierHasScope  atomic.Bool
	discoveryCalls   atomic.Int32
	listIndexesCalls atomic.Int32
	getLogCalls      atomic.Int32
	listLogsCalls    atomic.Int32
	applyCalls       atomic.Int32
	applyFn          func(context.Context, *ledgerpb.ApplyRequest) (*domain.ApplyResult, error)
}

func (s *authInterceptorBucketServer) Barrier(ctx context.Context, _ *ledgerpb.BarrierRequest) (*ledgerpb.BarrierResponse, error) {
	s.barrierCalls.Add(1)
	s.barrierHasScope.Store(internalauth.HasScope(internalauth.ExpandedScopesFromContext(ctx), internalauth.ScopeOpsRead))

	return &ledgerpb.BarrierResponse{}, nil
}

func (s *authInterceptorBucketServer) Discovery(context.Context, *ledgerpb.DiscoveryRequest) (*ledgerpb.DiscoveryResponse, error) {
	s.discoveryCalls.Add(1)

	return &ledgerpb.DiscoveryResponse{}, nil
}

func (s *authInterceptorBucketServer) ListIndexes(*ledgerpb.ListIndexesRequest, ledgerpb.BucketService_ListIndexesServer) error {
	s.listIndexesCalls.Add(1)

	return nil
}

func (s *authInterceptorBucketServer) GetLog(context.Context, *ledgerpb.GetLogRequest) (*ledgerpb.Log, error) {
	s.getLogCalls.Add(1)

	return &ledgerpb.Log{}, nil
}

func (s *authInterceptorBucketServer) ListLogs(*ledgerpb.ListLogsRequest, ledgerpb.BucketService_ListLogsServer) error {
	s.listLogsCalls.Add(1)

	return nil
}

func (s *authInterceptorBucketServer) Apply(ctx context.Context, req *ledgerpb.ApplyRequest) (*ledgerpb.ApplyResponse, error) {
	s.applyCalls.Add(1)
	if s.applyFn != nil {
		if _, err := s.applyFn(ctx, req); err != nil {
			return nil, err
		}
	}

	return &ledgerpb.ApplyResponse{}, nil
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

	_, err := client.Barrier(context.Background(), &ledgerpb.BarrierRequest{})
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Zero(t, server.barrierCalls.Load(), "fixed-scope denial must precede handler invocation")

	stream, err := client.ListIndexes(context.Background(), &ledgerpb.ListIndexesRequest{
		Scope:  ledgerpb.ListIndexesRequest_SCOPE_LEDGER,
		Ledger: "main",
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.ErrorIs(t, err, io.EOF)
	require.EqualValues(t, 1, server.listIndexesCalls.Load())

	stream, err = client.ListIndexes(context.Background(), &ledgerpb.ListIndexesRequest{
		Scope: ledgerpb.ListIndexesRequest_SCOPE_ALL,
	})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.EqualValues(t, 1, server.listIndexesCalls.Load(), "first-message denial must precede handler invocation")

	invalidToken := metadata.AppendToOutgoingContext(context.Background(), "authorization", "Bearer invalid")
	_, err = client.Discovery(invalidToken, &ledgerpb.DiscoveryRequest{})
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

	_, err := client.Barrier(context.Background(), &ledgerpb.BarrierRequest{})
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
		_, err := client.AggregateVolumes(ctx, &ledgerpb.AggregateVolumesRequest{}, ggrpc.Trailer(&trailer))
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.NotEmpty(t, trailer.Get(metadataKeyQueryProfileResult))
	})

	t.Run("unprofiled unary", func(t *testing.T) {
		var trailer metadata.MD
		_, err := client.Barrier(ctx, &ledgerpb.BarrierRequest{}, ggrpc.Trailer(&trailer))
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.NotEmpty(t, trailer.Get(metadataKeyQueryProfileResult))
	})

	t.Run("stream", func(t *testing.T) {
		stream, err := client.ListTransactions(ctx, &ledgerpb.ListTransactionsRequest{})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.NotEmpty(t, stream.Trailer().Get(metadataKeyQueryProfileResult))
	})
}

func newAuthInterceptorServer(t *testing.T, mode ServiceAuthPolicy, cfg internalauth.AuthConfig) (*authInterceptorBucketServer, ledgerpb.BucketServiceClient) {
	t.Helper()

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server, err := NewServiceServer(mode, cfg, "", 0, noopLogger{}, false, time.Second, nil, true, WithListener(listener))
	require.NoError(t, err)
	implementation := &authInterceptorBucketServer{}
	ledgerpb.RegisterBucketServiceServer(server.GetServer(), implementation)
	if mode == ServiceAuthPolicyPublic {
		ledgerpb.RegisterClusterServiceServer(server.GetServer(), &ledgerpb.UnimplementedClusterServiceServer{})
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

	return implementation, ledgerpb.NewBucketServiceClient(conn)
}

func TestRestoreServiceDoesNotInstallPublicJWTAuthorization(t *testing.T) {
	t.Parallel()

	server, client := newAuthInterceptorServer(t, ServiceAuthPolicyRestore, internalauth.AuthConfig{Enabled: true})
	_, err := client.Barrier(context.Background(), &ledgerpb.BarrierRequest{})
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
			name: "cluster internal secret",
			cfg:  internalauth.AuthConfig{Enabled: true, ClusterSecret: "cluster-secret"},
			ctx: metadata.AppendToOutgoingContext(
				context.Background(), "authorization", "Bearer cluster-secret",
			),
			wantCode: codes.OK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, test.cfg)
			_, err := client.Barrier(test.ctx, &ledgerpb.BarrierRequest{})
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

	_, err := client.Barrier(ctx, &ledgerpb.BarrierRequest{})
	require.Equal(t, codes.PermissionDenied, status.Code(err))
	require.Zero(t, server.barrierCalls.Load())
}

func TestListLogsAndGetLogKeepDistinctFixedScopes(t *testing.T) {
	t.Parallel()

	t.Run("ledger read lists logs but cannot fetch a bucket log", func(t *testing.T) {
		t.Parallel()

		server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, anonymousAuthConfig(internalauth.ScopeLedgersRead))
		stream, err := client.ListLogs(context.Background(), &ledgerpb.ListLogsRequest{})
		require.NoError(t, err)
		_, err = stream.Recv()
		require.ErrorIs(t, err, io.EOF)
		require.EqualValues(t, 1, server.listLogsCalls.Load())

		_, err = client.GetLog(context.Background(), &ledgerpb.GetLogRequest{})
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.Zero(t, server.getLogCalls.Load())
	})

	t.Run("ops read fetches a bucket log but cannot list ledger logs", func(t *testing.T) {
		t.Parallel()

		server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, anonymousAuthConfig(internalauth.ScopeOpsRead))
		_, err := client.GetLog(context.Background(), &ledgerpb.GetLogRequest{})
		require.NoError(t, err)
		require.EqualValues(t, 1, server.getLogCalls.Load())

		stream, err := client.ListLogs(context.Background(), &ledgerpb.ListLogsRequest{})
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
		request   *ledgerpb.Request
		granted   internalauth.Scope
		wantCode  codes.Code
		wantScope internalauth.Scope
	}{
		{"numscript save with LedgerWrite", &ledgerpb.Request{Type: &ledgerpb.Request_SaveNumscript{SaveNumscript: &ledgerpb.SaveNumscriptRequest{}}}, internalauth.ScopeLedgersWrite, codes.OK, ""},
		{"numscript save with OpsWrite", &ledgerpb.Request{Type: &ledgerpb.Request_SaveNumscript{SaveNumscript: &ledgerpb.SaveNumscriptRequest{}}}, internalauth.ScopeOpsWrite, codes.Unauthenticated, internalauth.ScopeLedgersWrite},
		{"add account type with MetadataWrite", &ledgerpb.Request{Type: &ledgerpb.Request_AddAccountType{AddAccountType: &ledgerpb.AddAccountTypeLedgerRequest{}}}, internalauth.ScopeMetadataWrite, codes.OK, ""},
		{"add account type with OpsWrite", &ledgerpb.Request{Type: &ledgerpb.Request_AddAccountType{AddAccountType: &ledgerpb.AddAccountTypeLedgerRequest{}}}, internalauth.ScopeOpsWrite, codes.Unauthenticated, internalauth.ScopeMetadataWrite},
		{"save ledger metadata with MetadataWrite", &ledgerpb.Request{Type: &ledgerpb.Request_SaveLedgerMetadata{SaveLedgerMetadata: &ledgerpb.SaveLedgerMetadataRequest{}}}, internalauth.ScopeMetadataWrite, codes.OK, ""},
		{"checkpoint with ClusterWrite", &ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{}}}, internalauth.ScopeClusterWrite, codes.OK, ""},
		{"checkpoint with OpsWrite", &ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{}}}, internalauth.ScopeOpsWrite, codes.Unauthenticated, internalauth.ScopeClusterWrite},
		{"delete checkpoint with OpsWrite", &ledgerpb.Request{Type: &ledgerpb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &ledgerpb.DeleteQueryCheckpointRequest{}}}, internalauth.ScopeOpsWrite, codes.Unauthenticated, internalauth.ScopeClusterWrite},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, anonymousAuthConfig(test.granted))
			_, err := client.Apply(context.Background(), ledgerpb.UnsignedApplyRequest("", test.request))
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
	_, err := client.Apply(context.Background(), ledgerpb.UnsignedApplyRequest("",
		&ledgerpb.Request{Type: &ledgerpb.Request_AddAccountType{
			AddAccountType: &ledgerpb.AddAccountTypeLedgerRequest{},
		}},
		&ledgerpb.Request{Type: &ledgerpb.Request_CreateQueryCheckpoint{
			CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{},
		}},
	))
	require.Equal(t, codes.Unauthenticated, status.Code(err))
	require.Contains(t, status.Convert(err).Message(), "request 1 requires scope "+string(internalauth.ScopeClusterWrite))
	require.Zero(t, server.applyCalls.Load())
}

func TestDistinctApplyScopesPreservesFirstRequestOrder(t *testing.T) {
	t.Parallel()

	requests := []*ledgerpb.Request{
		{Type: &ledgerpb.Request_AddAccountType{AddAccountType: &ledgerpb.AddAccountTypeLedgerRequest{}}},
		{Type: &ledgerpb.Request_AddAccountType{AddAccountType: &ledgerpb.AddAccountTypeLedgerRequest{}}},
		{Type: &ledgerpb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &ledgerpb.CreateQueryCheckpointRequest{}}},
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
		request  *ledgerpb.ApplyRequest
		wantCode codes.Code
	}{
		{
			name: "authorized request",
			request: ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
				Type: &ledgerpb.Request_AddAccountType{},
			}),
			wantCode: codes.OK,
		},
		{
			name: "unauthorized embedded request",
			request: ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
				Type: &ledgerpb.Request_CreateQueryCheckpoint{},
			}),
			wantCode: codes.Unauthenticated,
		},
		{
			name:     "malformed unsigned payload",
			request:  &ledgerpb.ApplyRequest{},
			wantCode: codes.InvalidArgument,
		},
		{
			name: "malformed signed payload reaches signature admission",
			request: ledgerpb.SignedApplyRequest(&ledgerpb.SignedApplyBatch{
				Payload: []byte{0xff},
			}),
			wantCode: codes.OK,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			authorizedCtx, err := authorizeDynamicUnaryRPC(ctx, test.request, ledgerpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_APPLY)
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

	invalidSignature := ledgerpb.SignedApplyRequest(&ledgerpb.SignedApplyBatch{
		KeyId:     "test-key",
		Payload:   []byte{0xff},
		Signature: make([]byte, ed25519.SignatureSize),
	})
	_, err = client.Apply(context.Background(), invalidSignature)
	require.Equal(t, codes.PermissionDenied, status.Code(err))

	malformedPayload := []byte{0xff}
	validSignature := ledgerpb.SignedApplyRequest(&ledgerpb.SignedApplyBatch{
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
	for number, name := range ledgerpb.AuthScope_name {
		scope := ledgerpb.AuthScope(number)
		if scope == ledgerpb.AuthScope_AUTH_SCOPE_UNSPECIFIED {
			continue
		}

		internal, err := authScope(scope)
		require.NoError(t, err, name)
		mapped[internal] = struct{}{}
	}

	require.Equal(t, internalauth.AllGranularScopes, mapped)
	_, err := authScope(ledgerpb.AuthScope(999))
	require.Equal(t, codes.Internal, status.Code(err))
}
