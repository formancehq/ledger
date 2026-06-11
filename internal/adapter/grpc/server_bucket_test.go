package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestAdoptForwardedCallerIfTrusted_TrustsClusterInternal verifies that when
// the request authenticated via the cluster-secret (peer-to-peer trust
// boundary), the leader picks up the forwarded caller identity so the audit
// entry can attribute the write to the original user (regression for #362).
func TestAdoptForwardedCallerIfTrusted_TrustsClusterInternal(t *testing.T) {
	t.Parallel()

	identity := &commonpb.CallerIdentity{
		Subject: "alice",
		Source:  &commonpb.CallerIdentity_Issuer{Issuer: "https://idp.example.com"},
	}
	req := &servicepb.ApplyRequest{ForwardedCaller: identity}

	ctx := internalauth.WithClusterInternal(context.Background(), true)
	out := adoptForwardedCallerIfTrusted(ctx, req)

	require.Same(t, identity, internalauth.ForwardedCallerFromContext(out))
}

// TestAdoptForwardedCallerIfTrusted_IgnoresFromRegularClient verifies that a
// regular (non-cluster-internal) client cannot spoof the audit identity by
// setting forwarded_caller. The field MUST be dropped on direct requests.
func TestAdoptForwardedCallerIfTrusted_IgnoresFromRegularClient(t *testing.T) {
	t.Parallel()

	req := &servicepb.ApplyRequest{
		ForwardedCaller: &commonpb.CallerIdentity{Subject: "attacker"},
	}

	out := adoptForwardedCallerIfTrusted(context.Background(), req)

	require.Nil(t, internalauth.ForwardedCallerFromContext(out),
		"non-cluster-internal requests must not be allowed to set the forwarded caller")
}

// TestAdoptForwardedCallerIfTrusted_NilForwardedNoOp verifies that an empty
// forwarded_caller leaves the context untouched even on a trusted hop, so the
// leader falls back to building the identity from its own claims (or nil).
func TestAdoptForwardedCallerIfTrusted_NilForwardedNoOp(t *testing.T) {
	t.Parallel()

	req := &servicepb.ApplyRequest{}

	ctx := internalauth.WithClusterInternal(context.Background(), true)
	out := adoptForwardedCallerIfTrusted(ctx, req)

	require.Nil(t, internalauth.ForwardedCallerFromContext(out))
}
