package grpc

import (
	"context"
	"crypto/subtle"
	"strings"
	"time"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
)

type queryProfileClock struct {
	start   time.Time
	claimed bool
}

type queryProfileClockKey struct{}
type applyBatchSizeKey struct{}

type indexedScope struct {
	scope internalauth.Scope
	index int
}

func queryProfileClockUnaryInterceptor(logger logging.Logger, slowThreshold time.Duration) ggrpc.UnaryServerInterceptor {
	return queryProfileClockUnaryInterceptorAt(logger, slowThreshold, time.Now)
}

func queryProfileClockUnaryInterceptorAt(logger logging.Logger, slowThreshold time.Duration, now func() time.Time) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		clock := &queryProfileClock{start: now()}
		ctx = context.WithValue(ctx, queryProfileClockKey{}, clock)
		resp, err := handler(ctx, req)
		if err != nil && wantsProfile(ctx) && !clock.claimed {
			_, profile := query.WithProfileStartingAt(ctx, clock.start)
			emitQueryProfile(ctx, profile, logger, slowThreshold)
		}

		return resp, err
	}
}

func queryProfileClockStreamInterceptor(logger logging.Logger, slowThreshold time.Duration) ggrpc.StreamServerInterceptor {
	return func(srv any, stream ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		clock := &queryProfileClock{start: time.Now()}
		ctx := context.WithValue(stream.Context(), queryProfileClockKey{}, clock)
		wrapped := &authServerStream{ServerStream: stream, ctx: ctx}
		err := handler(srv, wrapped)
		if err != nil && wantsProfile(ctx) && !clock.claimed {
			_, profile := query.WithProfileStartingAt(ctx, clock.start)
			emitQueryProfile(ctx, profile, logger, slowThreshold)
		}

		return err
	}
}

func authUnaryInterceptor(cfg internalauth.AuthConfig, peerOnly bool) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		if !peerOnly {
			if err := rejectDirectClusterSecret(ctx, cfg); err != nil {
				return nil, err
			}
		}
		if peerOnly {
			if err := requirePeerCredential(ctx, cfg); err != nil {
				return nil, err
			}
			ctx = internalauth.WithClusterInternal(ctx, true)
		}
		policy, protected, err := protectedRPCPolicy(info.FullMethod)
		if err != nil {
			return nil, err
		}
		if !protected {
			return handler(ctx, req)
		}

		ctx, err = evaluatePeerOrUserCredentials(ctx, cfg, peerOnly)
		if err != nil {
			return nil, err
		}
		ctx, err = authorizeUnaryRPC(ctx, req, policy)
		if err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func authStreamInterceptor(cfg internalauth.AuthConfig, peerOnly bool) ggrpc.StreamServerInterceptor {
	return func(srv any, stream ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		ctx := stream.Context()
		if peerOnly {
			if err := requirePeerCredential(ctx, cfg); err != nil {
				return err
			}
			ctx = internalauth.WithClusterInternal(ctx, true)
		} else if err := rejectDirectClusterSecret(ctx, cfg); err != nil {
			return err
		}

		policy, protected, err := protectedRPCPolicy(info.FullMethod)
		if err != nil {
			return err
		}
		if !protected {
			return handler(srv, &authServerStream{ServerStream: stream, ctx: ctx})
		}

		ctx, err = evaluatePeerOrUserCredentials(ctx, cfg, peerOnly)
		if err != nil {
			return err
		}
		authStream := &authServerStream{ServerStream: stream, ctx: ctx}
		stream = authStream

		switch typed := policy.GetPolicy().(type) {
		case *commonpb.MethodAuthPolicy_FixedScope:
			scope, err := authScope(typed.FixedScope)
			if err != nil {
				return err
			}
			if err := internalauth.AuthorizeGRPC(ctx, scope); err != nil {
				return err
			}
		case *commonpb.MethodAuthPolicy_DynamicResolver:
			if typed.DynamicResolver != commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_LIST_INDEXES {
				return status.Errorf(codes.Internal, "dynamic resolver %s is not valid for a streaming RPC", typed.DynamicResolver)
			}
			stream = &listIndexesAuthServerStream{authServerStream: authStream}
		default:
			return status.Error(codes.Internal, "RPC authentication policy is missing")
		}

		return handler(srv, stream)
	}
}

func rejectDirectClusterSecret(ctx context.Context, cfg internalauth.AuthConfig) error {
	if cfg.ClusterSecret == "" {
		return nil
	}
	token, presented, err := internalauth.BearerTokenFromGRPCContext(ctx)
	if err != nil {
		return err
	}
	if presented && len(token) == len(cfg.ClusterSecret) && subtle.ConstantTimeCompare([]byte(token), []byte(cfg.ClusterSecret)) == 1 {
		return status.Error(codes.Unauthenticated, "cluster secret is reserved for inter-node forwarding")
	}

	return nil
}

func requirePeerCredential(ctx context.Context, cfg internalauth.AuthConfig) error {
	// Auth-disabled clusters historically had no shared secret. They still get
	// a dedicated peer listener, but there is no credential to validate in that
	// mode. Authenticated clusters must present the cluster-secret here; a JWT
	// is never accepted as a peer credential.
	if cfg.ClusterSecret == "" && !cfg.Enabled {
		return nil
	}

	if cfg.ClusterSecret == "" {
		return status.Error(codes.Unauthenticated, "cluster secret is required on the peer service")
	}

	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return status.Error(codes.Unauthenticated, "peer transport credential is required")
	}
	values := md.Get(transport.PeerAuthMetadataKey)
	if len(values) == 1 && strings.HasPrefix(values[0], "Bearer ") {
		token := strings.TrimPrefix(values[0], "Bearer ")
		if token == "" || strings.ContainsAny(token, " \t\r\n") {
			return status.Error(codes.Unauthenticated, "invalid peer transport credential")
		}
		if len(token) == len(cfg.ClusterSecret) && subtle.ConstantTimeCompare([]byte(token), []byte(cfg.ClusterSecret)) == 1 {
			return nil
		}
	}

	return status.Error(codes.Unauthenticated, "invalid peer transport credential")
}

func evaluatePeerOrUserCredentials(ctx context.Context, cfg internalauth.AuthConfig, peerOnly bool) (context.Context, error) {
	if !peerOnly {
		return internalauth.EvaluateGRPCCredentials(ctx, cfg)
	}

	// A peer transport is authenticated independently of the caller. When a
	// forwarded user token is present, evaluate it normally. Calls without one
	// are internal node actions and retain the existing system-level scope
	// behavior without treating CLUSTER_SECRET as a user bearer token.
	_, presented, err := internalauth.BearerTokenFromGRPCContext(ctx)
	if err != nil {
		return ctx, err
	}
	if presented {
		return internalauth.EvaluateGRPCCredentials(ctx, cfg)
	}

	if !cfg.Enabled {
		return internalauth.EvaluateGRPCCredentials(ctx, cfg)
	}

	return internalauth.WithPeerSystemAuthentication(ctx), nil
}

func protectedRPCPolicy(method string) (*commonpb.MethodAuthPolicy, bool, error) {
	if isInfrastructureRPCMethod(method) {
		return nil, false, nil
	}

	policy, err := commonpb.RPCAuthPolicyForMethod(method)
	if err != nil {
		return nil, false, status.Error(codes.Internal, err.Error())
	}
	if public, ok := policy.GetPolicy().(*commonpb.MethodAuthPolicy_Public); ok {
		if !public.Public {
			return nil, false, status.Error(codes.Internal, "invalid public RPC authentication policy")
		}

		return nil, false, nil
	}

	return policy, true, nil
}

func authorizeUnaryRPC(ctx context.Context, req any, policy *commonpb.MethodAuthPolicy) (context.Context, error) {
	switch typed := policy.GetPolicy().(type) {
	case *commonpb.MethodAuthPolicy_FixedScope:
		scope, err := authScope(typed.FixedScope)
		if err != nil {
			return ctx, err
		}

		return ctx, internalauth.AuthorizeGRPC(ctx, scope)
	case *commonpb.MethodAuthPolicy_DynamicResolver:
		return authorizeDynamicUnaryRPC(ctx, req, typed.DynamicResolver)
	default:
		return ctx, status.Error(codes.Internal, "RPC authentication policy is missing")
	}
}

func authorizeDynamicUnaryRPC(ctx context.Context, req any, resolver commonpb.DynamicAuthResolver) (context.Context, error) {
	switch resolver {
	case commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_APPLY:
		applyReq, ok := req.(*servicepb.ApplyRequest)
		if !ok {
			return ctx, unexpectedAuthRequest(resolver, req)
		}

		batch, err := servicepb.PeekBatch(applyReq)
		if err != nil {
			if applyReq.GetSigned() != nil {
				return ctx, nil
			}

			return ctx, status.Errorf(codes.InvalidArgument, "%v", err)
		}
		if len(batch.GetRequests()) == 0 {
			return ctx, errEnvelopesRequired
		}
		for _, required := range distinctApplyScopes(batch.GetRequests()) {
			if err := internalauth.AuthorizeGRPC(ctx, required.scope); err != nil {
				return ctx, status.Errorf(status.Code(err), "request %d requires scope %s", required.index, required.scope)
			}
		}

		return context.WithValue(ctx, applyBatchSizeKey{}, len(batch.GetRequests())), nil
	case commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_GET_INDEX:
		indexReq, ok := req.(*servicepb.GetIndexRequest)
		if !ok {
			return ctx, unexpectedAuthRequest(resolver, req)
		}

		return ctx, internalauth.AuthorizeGRPC(ctx, indexAuthScope(indexReq.GetLedger()))
	case commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_GET_INDEX_ENTRY_STATUS:
		entryReq, ok := req.(*servicepb.GetIndexEntryStatusRequest)
		if !ok {
			return ctx, unexpectedAuthRequest(resolver, req)
		}

		return ctx, internalauth.AuthorizeGRPC(ctx, indexAuthScope(entryReq.GetLedger()))
	default:
		return ctx, status.Errorf(codes.Internal, "dynamic resolver %s is not valid for a unary RPC", resolver)
	}
}

func distinctApplyScopes(requests []*servicepb.Request) []indexedScope {
	seen := make(map[internalauth.Scope]struct{})
	required := make([]indexedScope, 0, len(requests))
	for index, request := range requests {
		scope := internalauth.RequiredScopeForRequest(request)
		if _, exists := seen[scope]; exists {
			continue
		}
		seen[scope] = struct{}{}
		required = append(required, indexedScope{scope: scope, index: index})
	}

	return required
}

func unexpectedAuthRequest(resolver commonpb.DynamicAuthResolver, req any) error {
	return status.Errorf(codes.Internal, "dynamic resolver %s received %T", resolver, req)
}

func indexAuthScope(ledger string) internalauth.Scope {
	if ledger != "" {
		return internalauth.ScopeLedgersRead
	}

	return internalauth.ScopeOpsRead
}

func authScope(scope commonpb.AuthScope) (internalauth.Scope, error) {
	switch scope {
	case commonpb.AuthScope_AUTH_SCOPE_LEDGER_READ:
		return internalauth.ScopeLedgersRead, nil
	case commonpb.AuthScope_AUTH_SCOPE_LEDGER_WRITE:
		return internalauth.ScopeLedgersWrite, nil
	case commonpb.AuthScope_AUTH_SCOPE_TRANSACTION_READ:
		return internalauth.ScopeTransactionsRead, nil
	case commonpb.AuthScope_AUTH_SCOPE_TRANSACTION_WRITE:
		return internalauth.ScopeTransactionsWrite, nil
	case commonpb.AuthScope_AUTH_SCOPE_ACCOUNT_READ:
		return internalauth.ScopeAccountsRead, nil
	case commonpb.AuthScope_AUTH_SCOPE_METADATA_WRITE:
		return internalauth.ScopeMetadataWrite, nil
	case commonpb.AuthScope_AUTH_SCOPE_AUDIT_READ:
		return internalauth.ScopeAuditRead, nil
	case commonpb.AuthScope_AUTH_SCOPE_AUDIT_WRITE:
		return internalauth.ScopeAuditWrite, nil
	case commonpb.AuthScope_AUTH_SCOPE_OPS_READ:
		return internalauth.ScopeOpsRead, nil
	case commonpb.AuthScope_AUTH_SCOPE_OPS_WRITE:
		return internalauth.ScopeOpsWrite, nil
	case commonpb.AuthScope_AUTH_SCOPE_QUERY_READ:
		return internalauth.ScopeQueriesRead, nil
	case commonpb.AuthScope_AUTH_SCOPE_QUERY_WRITE:
		return internalauth.ScopeQueriesWrite, nil
	case commonpb.AuthScope_AUTH_SCOPE_CLUSTER_READ:
		return internalauth.ScopeClusterRead, nil
	case commonpb.AuthScope_AUTH_SCOPE_CLUSTER_WRITE:
		return internalauth.ScopeClusterWrite, nil
	default:
		return "", status.Errorf(codes.Internal, "unknown RPC authentication scope %s", scope)
	}
}

type authServerStream struct {
	ggrpc.ServerStream

	ctx context.Context
}

func (s *authServerStream) Context() context.Context {
	return s.ctx
}

type listIndexesAuthServerStream struct {
	*authServerStream

	authorized bool
}

func (s *listIndexesAuthServerStream) RecvMsg(message any) error {
	if err := s.ServerStream.RecvMsg(message); err != nil {
		return err
	}
	if s.authorized {
		return nil
	}

	req, ok := message.(*servicepb.ListIndexesRequest)
	if !ok {
		return status.Errorf(codes.Internal, "ListIndexes authentication received %T", message)
	}
	if err := internalauth.AuthorizeGRPC(s.ctx, indexAuthScopeForList(req)); err != nil {
		return err
	}
	s.authorized = true

	return nil
}

func indexAuthScopeForList(req *servicepb.ListIndexesRequest) internalauth.Scope {
	if req.GetScope() == servicepb.ListIndexesRequest_SCOPE_LEDGER {
		return internalauth.ScopeLedgersRead
	}

	return internalauth.ScopeOpsRead
}
