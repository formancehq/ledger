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
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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

// TestListLogs_RequiresLedgersReadScope guards EN-1508: listing a ledger's logs
// is a ledger-scoped read and must require ledger:LedgerRead across transports,
// matching the HTTP route (which already sits behind requireLedgersRead). The
// MockController carries no expectations on the deny paths, so any controller
// call would fail gomock — proving the scope gate short-circuits before the
// controller is reached.
func TestListLogs_RequiresLedgersReadScope(t *testing.T) {
	t.Parallel()

	// anonCfg enables auth and grants the given scopes to unauthenticated
	// callers via the anonymous mapping (see TestBarrier for the rationale).
	anonCfg := func(scopes ...internalauth.Scope) internalauth.AuthConfig {
		return internalauth.AuthConfig{
			Enabled: true,
			ScopeMapping: internalauth.ScopeMapping{
				internalauth.ScopeMappingAnonymousKey: scopes,
			},
		}
	}

	// newImpl wires a MockController. expectList=true expects ListLogs once
	// (the authorized path); expectList=false leaves the controller with no
	// expectations so any call fails gomock, proving auth denied before it.
	newImpl := func(cfg internalauth.AuthConfig, expectList bool) *BucketServiceServerImpl {
		controller := NewMockController(gomock.NewController(t))
		if expectList {
			controller.EXPECT().
				ListLogs(gomock.Any(), "main", gomock.Any(), gomock.Any(), gomock.Any()).
				Return(page[commonpb.Log](), nil)
		}

		return &BucketServiceServerImpl{logger: noopLogger{}, ctrl: controller, authCfg: cfg}
	}

	// A minimal valid request: with no options the checkpoint id is 0 (live
	// read) and min_log_sequence is 0, so the authorized path reaches the
	// controller without touching the read store.
	newReq := func() *servicepb.ListLogsRequest { return &servicepb.ListLogsRequest{Ledger: "main"} }

	t.Run("allows caller with ScopeLedgersRead", func(t *testing.T) {
		t.Parallel()

		impl := newImpl(anonCfg(internalauth.ScopeLedgersRead), true)

		require.NoError(t, impl.ListLogs(newReq(), newFakeServerStream[commonpb.Log](t)))
	})

	t.Run("rejects caller whose scopes omit ledger-read", func(t *testing.T) {
		t.Parallel()

		// Effective scopes carry no ledger/log read scope at all.
		impl := newImpl(anonCfg(internalauth.ScopeAccountsRead, internalauth.ScopeTransactionsRead), false)

		require.Error(t, impl.ListLogs(newReq(), newFakeServerStream[commonpb.Log](t)))
	})

	t.Run("rejects ScopeOpsRead-only caller", func(t *testing.T) {
		t.Parallel()

		// Regression guard: the scope moved off OpsRead, so an OpsRead-only
		// token is now denied for ListLogs.
		impl := newImpl(anonCfg(internalauth.ScopeOpsRead), false)

		require.Error(t, impl.ListLogs(newReq(), newFakeServerStream[commonpb.Log](t)))
	})

	t.Run("rejects unauthenticated caller", func(t *testing.T) {
		t.Parallel()

		impl := newImpl(anonCfg(), false) // anonymous gets no scopes

		err := impl.ListLogs(newReq(), newFakeServerStream[commonpb.Log](t))
		require.Error(t, err)
		require.Equal(t, codes.Unauthenticated, status.Code(err))
	})
}

// TestGetLog_RequiresOpsReadScope pins the other half of the EN-1508 split:
// the global GetLog(sequence) addresses a bucket-wide raft sequence with no
// ledger identity, so it stays on ledger:OpsRead and must NOT accept a
// ledger-read-only token. This proves the two operations did not collapse onto
// the same scope.
func TestGetLog_RequiresOpsReadScope(t *testing.T) {
	t.Parallel()

	anonCfg := func(scopes ...internalauth.Scope) internalauth.AuthConfig {
		return internalauth.AuthConfig{
			Enabled: true,
			ScopeMapping: internalauth.ScopeMapping{
				internalauth.ScopeMappingAnonymousKey: scopes,
			},
		}
	}

	newImpl := func(cfg internalauth.AuthConfig, expectGet bool) *BucketServiceServerImpl {
		controller := NewMockController(gomock.NewController(t))
		if expectGet {
			// checkpoint id 0 → readController returns the live controller, so
			// the authorized path reaches this expectation directly.
			controller.EXPECT().GetLog(gomock.Any(), uint64(1)).Return(&commonpb.Log{}, nil)
		}

		return &BucketServiceServerImpl{logger: noopLogger{}, ctrl: controller, authCfg: cfg}
	}

	t.Run("allows caller with ScopeOpsRead", func(t *testing.T) {
		t.Parallel()

		impl := newImpl(anonCfg(internalauth.ScopeOpsRead), true)

		_, err := impl.GetLog(context.Background(), &servicepb.GetLogRequest{Sequence: 1})
		require.NoError(t, err)
	})

	t.Run("rejects ScopeLedgersRead-only caller", func(t *testing.T) {
		t.Parallel()

		impl := newImpl(anonCfg(internalauth.ScopeLedgersRead), false)

		_, err := impl.GetLog(context.Background(), &servicepb.GetLogRequest{Sequence: 1})
		require.Error(t, err)
	})
}
