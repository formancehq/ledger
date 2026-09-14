package grpc

import (
	"context"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func authUnaryInterceptor(cfg internalauth.AuthConfig) ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		if isInfrastructureRPCMethod(info.FullMethod) {
			return handler(ctx, req)
		}

		policy, err := commonpb.RPCAuthPolicyForMethod(info.FullMethod)
		if err != nil {
			return nil, status.Error(codes.Internal, err.Error())
		}
		if public, ok := policy.GetPolicy().(*commonpb.MethodAuthPolicy_Public); ok {
			if !public.Public {
				return nil, status.Error(codes.Internal, "invalid public RPC authentication policy")
			}

			return handler(ctx, req)
		}

		ctx, err = internalauth.EvaluateGRPCCredentials(ctx, cfg)
		if err != nil {
			return nil, err
		}
		if err := authorizeUnaryRPC(ctx, req, policy); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func authStreamInterceptor(cfg internalauth.AuthConfig) ggrpc.StreamServerInterceptor {
	return func(srv any, stream ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		if isInfrastructureRPCMethod(info.FullMethod) {
			return handler(srv, stream)
		}

		policy, err := commonpb.RPCAuthPolicyForMethod(info.FullMethod)
		if err != nil {
			return status.Error(codes.Internal, err.Error())
		}
		if public, ok := policy.GetPolicy().(*commonpb.MethodAuthPolicy_Public); ok {
			if !public.Public {
				return status.Error(codes.Internal, "invalid public RPC authentication policy")
			}

			return handler(srv, stream)
		}

		ctx, err := internalauth.EvaluateGRPCCredentials(stream.Context(), cfg)
		if err != nil {
			return err
		}
		stream = &authServerStream{ServerStream: stream, ctx: ctx}

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
			stream = &listIndexesAuthServerStream{authServerStream: stream.(*authServerStream)}
		default:
			return status.Error(codes.Internal, "RPC authentication policy is missing")
		}

		return handler(srv, stream)
	}
}

func authorizeUnaryRPC(ctx context.Context, req any, policy *commonpb.MethodAuthPolicy) error {
	switch typed := policy.GetPolicy().(type) {
	case *commonpb.MethodAuthPolicy_FixedScope:
		scope, err := authScope(typed.FixedScope)
		if err != nil {
			return err
		}

		return internalauth.AuthorizeGRPC(ctx, scope)
	case *commonpb.MethodAuthPolicy_DynamicResolver:
		return authorizeDynamicUnaryRPC(ctx, req, typed.DynamicResolver)
	default:
		return status.Error(codes.Internal, "RPC authentication policy is missing")
	}
}

func authorizeDynamicUnaryRPC(ctx context.Context, req any, resolver commonpb.DynamicAuthResolver) error {
	switch resolver {
	case commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_APPLY:
		applyReq, ok := req.(*servicepb.ApplyRequest)
		if !ok {
			return unexpectedAuthRequest(resolver, req)
		}

		batch, err := servicepb.PeekBatch(applyReq)
		if err != nil {
			if applyReq.GetSigned() != nil {
				return nil
			}

			return status.Errorf(codes.InvalidArgument, "%v", err)
		}
		if len(batch.GetRequests()) == 0 {
			return errEnvelopesRequired
		}
		for index, request := range batch.GetRequests() {
			required := internalauth.RequiredScopeForRequest(request)
			if err := internalauth.AuthorizeGRPC(ctx, required); err != nil {
				return status.Errorf(status.Code(err), "request %d requires scope %s", index, required)
			}
		}

		return nil
	case commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_GET_INDEX:
		indexReq, ok := req.(*servicepb.GetIndexRequest)
		if !ok {
			return unexpectedAuthRequest(resolver, req)
		}

		return internalauth.AuthorizeGRPC(ctx, indexAuthScope(indexReq.GetLedger()))
	case commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_GET_INDEX_ENTRY_STATUS:
		entryReq, ok := req.(*servicepb.GetIndexEntryStatusRequest)
		if !ok {
			return unexpectedAuthRequest(resolver, req)
		}

		return internalauth.AuthorizeGRPC(ctx, indexAuthScope(entryReq.GetLedger()))
	default:
		return status.Errorf(codes.Internal, "dynamic resolver %s is not valid for a unary RPC", resolver)
	}
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
