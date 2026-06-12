package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestAdoptForwardedSnapshotIfTrusted_TrustsClusterInternal verifies that
// when the request authenticated via the cluster-secret (peer-to-peer trust
// boundary), the leader picks up the forwarded caller snapshot so the audit
// entry can attribute the write to the original user (regression for #362 /
// EN-1079).
func TestAdoptForwardedSnapshotIfTrusted_TrustsClusterInternal(t *testing.T) {
	t.Parallel()

	snapshot := &commonpb.CallerSnapshot{
		Identity: &commonpb.CallerIdentity{
			Subject: "alice",
			Source:  &commonpb.CallerIdentity_Issuer{Issuer: "https://idp.example.com"},
		},
		Scopes: []string{"transactions:write"},
	}
	req := &servicepb.ApplyRequest{ForwardedCallerSnapshot: snapshot}

	ctx := internalauth.WithClusterInternal(context.Background(), true)
	out := adoptForwardedSnapshotIfTrusted(ctx, req)

	require.Same(t, snapshot, internalauth.ForwardedSnapshotFromContext(out))
}

// TestAdoptForwardedSnapshotIfTrusted_IgnoresFromRegularClient verifies that
// a regular (non-cluster-internal) client cannot spoof the audit identity by
// setting forwarded_caller. The field MUST be dropped on direct requests.
func TestAdoptForwardedSnapshotIfTrusted_IgnoresFromRegularClient(t *testing.T) {
	t.Parallel()

	req := &servicepb.ApplyRequest{
		ForwardedCallerSnapshot: &commonpb.CallerSnapshot{
			Identity: &commonpb.CallerIdentity{Subject: "attacker"},
		},
	}

	out := adoptForwardedSnapshotIfTrusted(context.Background(), req)

	require.Nil(t, internalauth.ForwardedSnapshotFromContext(out),
		"non-cluster-internal requests must not be allowed to set the forwarded snapshot")
}

// TestAdoptForwardedSnapshotIfTrusted_NilForwardedNoOp verifies that an
// empty forwarded_caller leaves the context untouched even on a trusted hop,
// so the leader falls back to building the snapshot from its own claims (or
// nil).
func TestAdoptForwardedSnapshotIfTrusted_NilForwardedNoOp(t *testing.T) {
	t.Parallel()

	req := &servicepb.ApplyRequest{}

	ctx := internalauth.WithClusterInternal(context.Background(), true)
	out := adoptForwardedSnapshotIfTrusted(ctx, req)

	require.Nil(t, internalauth.ForwardedSnapshotFromContext(out))
}
