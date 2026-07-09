package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

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
		Scopes: []string{"ledger:TransactionWrite"},
	}
	req := &servicepb.ApplyRequest{ForwardedCallerSnapshot: snapshot}
	impl := &BucketServiceServerImpl{logger: testLogger()}

	ctx := internalauth.WithClusterInternal(context.Background(), true)
	out, err := impl.adoptForwardedSnapshotIfTrusted(ctx, req)

	require.NoError(t, err)
	require.Same(t, snapshot, internalauth.ForwardedSnapshotFromContext(out))
}

// TestAdoptForwardedSnapshotIfTrusted_RejectsFromRegularClient verifies that a
// regular (non-cluster-internal) client cannot spoof the audit identity by
// setting forwarded_caller. A forwarded snapshot on an untrusted channel is
// rejected loudly rather than silently dropped, so a cluster-secret
// misconfiguration surfaces instead of corrupting the audit trail.
func TestAdoptForwardedSnapshotIfTrusted_RejectsFromRegularClient(t *testing.T) {
	t.Parallel()

	req := &servicepb.ApplyRequest{
		ForwardedCallerSnapshot: &commonpb.CallerSnapshot{
			Identity: &commonpb.CallerIdentity{Subject: "attacker"},
		},
	}
	impl := &BucketServiceServerImpl{logger: testLogger()}

	out, err := impl.adoptForwardedSnapshotIfTrusted(context.Background(), req)

	require.Equal(t, codes.PermissionDenied, status.Code(err),
		"a forwarded snapshot on a non-cluster-internal channel must be rejected")
	require.Nil(t, internalauth.ForwardedSnapshotFromContext(out),
		"the untrusted forwarded snapshot must not be adopted")
}

// TestAdoptForwardedSnapshotIfTrusted_NilForwardedNoOp verifies that an
// absent forwarded_caller leaves the context untouched (and errors nowhere),
// so a direct request or an unauthenticated hop falls back to building the
// snapshot from its own claims (or nil).
func TestAdoptForwardedSnapshotIfTrusted_NilForwardedNoOp(t *testing.T) {
	t.Parallel()

	req := &servicepb.ApplyRequest{}
	impl := &BucketServiceServerImpl{logger: testLogger()}

	// No forwarded snapshot is fine on both a trusted and a plain context.
	ctx := internalauth.WithClusterInternal(context.Background(), true)
	out, err := impl.adoptForwardedSnapshotIfTrusted(ctx, req)
	require.NoError(t, err)
	require.Nil(t, internalauth.ForwardedSnapshotFromContext(out))

	out, err = impl.adoptForwardedSnapshotIfTrusted(context.Background(), req)
	require.NoError(t, err)
	require.Nil(t, internalauth.ForwardedSnapshotFromContext(out))
}
