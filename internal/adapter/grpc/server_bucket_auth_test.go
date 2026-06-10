package grpc

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/ctrl"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// barrierStubController records Barrier calls. It embeds ctrl.Controller (nil)
// so only Barrier is implemented; any other method would panic, which is the
// point — the auth check must reject before reaching the controller.
type barrierStubController struct {
	ctrl.Controller

	calls int
}

func (s *barrierStubController) Barrier(context.Context) (uint64, error) {
	s.calls++

	return 7, nil
}

// TestBarrier_RequiresOpsReadScope guards the fix for the unauthenticated
// Barrier RPC: it proposes a no-op through Raft, so it must require ops:read
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

	newImpl := func(cfg internalauth.AuthConfig) (*BucketServiceServerImpl, *barrierStubController) {
		stub := &barrierStubController{}

		return &BucketServiceServerImpl{ctrl: stub, authCfg: cfg}, stub
	}

	t.Run("rejects unauthenticated caller", func(t *testing.T) {
		t.Parallel()

		impl, stub := newImpl(anonCfg()) // anonymous gets no scopes

		_, err := impl.Barrier(context.Background(), &servicepb.BarrierRequest{})
		require.Error(t, err)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
		require.Zero(t, stub.calls, "Barrier must not propose through Raft when unauthenticated")
	})

	t.Run("rejects caller whose scopes omit ops:read", func(t *testing.T) {
		t.Parallel()

		// Effective scopes lack ops:read — the wrong-scope case.
		impl, stub := newImpl(anonCfg(internalauth.ScopeAccountsRead, internalauth.ScopeTransactionsRead))

		_, err := impl.Barrier(context.Background(), &servicepb.BarrierRequest{})
		require.Error(t, err)
		require.Zero(t, stub.calls, "Barrier must not propose for an under-scoped caller")
	})

	t.Run("allows caller with ops:read", func(t *testing.T) {
		t.Parallel()

		impl, stub := newImpl(anonCfg(internalauth.ScopeOpsRead))

		resp, err := impl.Barrier(context.Background(), &servicepb.BarrierRequest{})
		require.NoError(t, err)
		require.Equal(t, uint64(7), resp.GetCommitIndex())
		require.Equal(t, 1, stub.calls)
	})

	t.Run("allows cluster-internal caller (leader forwarding)", func(t *testing.T) {
		t.Parallel()

		impl, stub := newImpl(internalauth.AuthConfig{Enabled: true, ClusterSecret: "cluster-secret"})
		ctx := metadata.NewIncomingContext(context.Background(),
			metadata.Pairs("authorization", "Bearer cluster-secret"))

		_, err := impl.Barrier(ctx, &servicepb.BarrierRequest{})
		require.NoError(t, err)
		require.Equal(t, 1, stub.calls)
	})

	t.Run("auth disabled allows caller", func(t *testing.T) {
		t.Parallel()

		impl, stub := newImpl(internalauth.AuthConfig{Enabled: false})

		_, err := impl.Barrier(context.Background(), &servicepb.BarrierRequest{})
		require.NoError(t, err)
		require.Equal(t, 1, stub.calls)
	})
}
