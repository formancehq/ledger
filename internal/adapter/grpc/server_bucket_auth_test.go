package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestBarrier_RequiresOpsReadScope guards the fix for the unauthenticated
// Barrier RPC: it proposes a no-op through Raft, so it must require ledger:OpsRead
// and must not reach the controller (i.e. must not propose) when the caller
// lacks it.
func TestBarrier_RequiresOpsReadScope(t *testing.T) {
	t.Parallel()

	// anonCfg enables auth and grants the given scopes to unauthenticated
	// (no-token) callers via the anonymous mapping, letting us exercise the
	// scope gate without minting JWTs (the JWT/scope matrix is covered by the
	// auth package's own tests).
	anonCfg := func(scopes ...internalauth.Scope) internalauth.AuthConfig {
		return internalauth.AuthConfig{
			Enabled: true,
			ScopeMapping: internalauth.ScopeMapping{
				internalauth.ScopeMappingAnonymousKey: scopes,
			},
		}
	}

	// newImpl returns a BucketServiceServerImpl wired to a MockController.
	// Tests that expect Barrier to be reached set expectBarrier=true; tests
	// that expect the auth gate to short-circuit set it to false so that any
	// proposed call would fail gomock's expectations.
	newImpl := func(cfg internalauth.AuthConfig, expectBarrier bool) *BucketServiceServerImpl {
		controller := NewMockController(gomock.NewController(t))
		if expectBarrier {
			controller.EXPECT().Barrier(gomock.Any()).Return(uint64(7), nil)
		}

		return &BucketServiceServerImpl{ctrl: controller, authCfg: cfg}
	}

	t.Run("rejects unauthenticated caller", func(t *testing.T) {
		t.Parallel()

		impl := newImpl(anonCfg(), false) // anonymous gets no scopes; Barrier must NOT be reached

		_, err := impl.Barrier(context.Background(), &servicepb.BarrierRequest{})
		require.Error(t, err)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	})

	t.Run("rejects caller whose scopes omit ledger:OpsRead", func(t *testing.T) {
		t.Parallel()

		// Effective scopes lack ledger:OpsRead — the wrong-scope case.
		impl := newImpl(anonCfg(internalauth.ScopeAccountsRead, internalauth.ScopeTransactionsRead), false)

		_, err := impl.Barrier(context.Background(), &servicepb.BarrierRequest{})
		require.Error(t, err)
	})

	t.Run("allows caller with ledger:OpsRead", func(t *testing.T) {
		t.Parallel()

		impl := newImpl(anonCfg(internalauth.ScopeOpsRead), true)

		resp, err := impl.Barrier(context.Background(), &servicepb.BarrierRequest{})
		require.NoError(t, err)
		require.Equal(t, uint64(7), resp.GetCommitIndex())
	})

	t.Run("allows cluster-internal caller (leader forwarding)", func(t *testing.T) {
		t.Parallel()

		impl := newImpl(internalauth.AuthConfig{Enabled: true, ClusterSecret: "cluster-secret"}, true)
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("authorization", "Bearer cluster-secret"))

		_, err := impl.Barrier(ctx, &servicepb.BarrierRequest{})
		require.NoError(t, err)
	})

	t.Run("auth disabled allows caller", func(t *testing.T) {
		t.Parallel()

		impl := newImpl(internalauth.AuthConfig{Enabled: false}, true)

		_, err := impl.Barrier(context.Background(), &servicepb.BarrierRequest{})
		require.NoError(t, err)
	})
}
