package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestHTTPProofRPCRequiresObservedIngressFromTrustedPeer(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		enabled bool
		token   string
		want    codes.Code
	}{
		{"public auth-disabled client", false, "", codes.PermissionDenied},
		{"public scoped client", true, "", codes.PermissionDenied},
		{"wrong peer secret", false, "wrong-secret", codes.PermissionDenied},
		{"auth-disabled matching peer", false, "peer-secret", codes.OK},
		{"authenticated matching peer", true, "peer-secret", codes.OK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cfg := anonymousAuthConfig(internalauth.ScopeMetadataWrite)
			cfg.Enabled = tc.enabled
			cfg.ClusterSecret = "peer-secret"
			server, client := newAuthInterceptorServer(t, ServiceAuthPolicyPublic, cfg)
			ctx := context.Background()
			if tc.token != "" {
				ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer "+tc.token))
			}
			_, err := client.Apply(ctx, &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_HttpSigned{HttpSigned: &signaturepb.SignedHTTPRequest{
				Method: "POST", RequestTarget: "/v3/main/transactions/42/metadata", Body: []byte(`{}`),
			}}})
			require.Equal(t, tc.want, status.Code(err))
			if tc.want == codes.OK {
				require.Equal(t, int32(1), server.applyCalls.Load())
			} else {
				require.Zero(t, server.applyCalls.Load(), "a public caller cannot submit a self-declared HTTP authority through gRPC")
			}
		})
	}
}
