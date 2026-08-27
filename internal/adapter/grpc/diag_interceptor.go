package grpc

import (
	"context"

	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/pkg/readdiag"
)

// metadataKeyDiag carries the serving diagnostics collected by readdiag as
// a single compact "k=v k=v" response trailer. The model workload copies it
// verbatim into finding details.
const metadataKeyDiag = "x-diag"

// diagInterceptor seeds a readdiag collector into the request context and
// exposes whatever the read path recorded as the x-diag response trailer.
func diagInterceptor() ggrpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *ggrpc.UnaryServerInfo, handler ggrpc.UnaryHandler) (any, error) {
		d := readdiag.New()
		resp, err := handler(readdiag.WithDiag(ctx, d), req)

		if s := d.String(); s != "" {
			_ = ggrpc.SetTrailer(ctx, metadata.Pairs(metadataKeyDiag, s))
		}

		return resp, err
	}
}

// diagStreamInterceptor is the streaming twin of diagInterceptor.
func diagStreamInterceptor() ggrpc.StreamServerInterceptor {
	return func(srv any, ss ggrpc.ServerStream, info *ggrpc.StreamServerInfo, handler ggrpc.StreamHandler) error {
		d := readdiag.New()
		ctx := readdiag.WithDiag(ss.Context(), d)

		err := handler(srv, &consistencyServerStream{ServerStream: ss, ctx: ctx})

		if s := d.String(); s != "" {
			ss.SetTrailer(metadata.Pairs(metadataKeyDiag, s))
		}

		return err
	}
}
