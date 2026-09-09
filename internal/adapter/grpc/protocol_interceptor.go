package grpc

import (
	"context"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
)

// checkProtocolVersion gates the service listener only, not Raft transport.
// Discovery, health and reflection remain available to incompatible clients.
// There is no authenticated-client bypass: internal forwarding also declares
// the protocol of the binary encoding the forwarded request.
func checkProtocolVersion(ctx context.Context, method string) error {
	if method == servicepb.BucketService_Discovery_FullMethodName ||
		strings.HasPrefix(method, "/grpc.health.v1.Health/") ||
		strings.HasPrefix(method, "/grpc.reflection.v1.ServerReflection/") ||
		strings.HasPrefix(method, "/grpc.reflection.v1alpha.ServerReflection/") {
		return nil
	}

	md, _ := metadata.FromIncomingContext(ctx)
	versions := md.Get(grpcprotocol.MetadataKey)
	if len(versions) == 1 && versions[0] == grpcprotocol.Version {
		return nil
	}

	// Do not echo unbounded, client-controlled metadata into the status message.
	return status.Errorf(codes.FailedPrecondition,
		"incompatible Ledger gRPC protocol: missing, invalid or unsupported version; required %s=%s; use a client built for this protocol (the ledgerctl shipped with the server)",
		grpcprotocol.MetadataKey, grpcprotocol.Version)
}

func protocolVersionInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := checkProtocolVersion(ctx, info.FullMethod); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func protocolVersionStreamInterceptor() grpc.StreamServerInterceptor {
	return func(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := checkProtocolVersion(stream.Context(), info.FullMethod); err != nil {
			return err
		}

		return handler(srv, stream)
	}
}
