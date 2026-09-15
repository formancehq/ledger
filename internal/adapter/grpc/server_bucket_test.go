package grpc

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestAnalyzeProgressEmitterSamplesDeterministicallyAndStopsAfterSendFailure(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("stream closed")
	var emitted []uint64
	canceled := false
	emitter := newAnalyzeProgressEmitter(func(processed, _ uint64) error {
		emitted = append(emitted, processed)
		if processed == 8 {
			return wantErr
		}

		return nil
	}, func() { canceled = true })
	for processed := uint64(1); processed <= 32; processed++ {
		emitter.report(processed, 0)
	}

	require.Equal(t, []uint64{1, 2, 4, 8}, emitted)
	require.ErrorIs(t, emitter.err(), wantErr)
	require.True(t, canceled)
}

func TestAnalyzeProgressEmitterHasABoundedLifetimeCardinality(t *testing.T) {
	t.Parallel()

	count := 0
	emitter := newAnalyzeProgressEmitter(func(_, _ uint64) error {
		count++

		return nil
	}, nil)
	for event := uint64(1); event <= 2048; event++ {
		emitter.report(event, 0)
	}
	require.Equal(t, 12, count)

	// The event ordinal is saturated, so even a theoretical uint64 overflow
	// cannot restart the power-of-two sequence. Across its full lifetime the
	// emitter can therefore produce at most 64 progress messages.
	emitter.seen = ^uint64(0) - 1
	emitter.report(2049, 0)
	emitter.report(2050, 0)
	require.Equal(t, 12, count)
}

func TestAnalyzeAccountsBoundsProgressAndKeepsTheFinalResult(t *testing.T) {
	t.Parallel()

	controller := NewMockController(gomock.NewController(t))
	controller.EXPECT().AnalyzeAccounts(gomock.Any(), "main", uint32(0), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, report func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
			for event := uint64(1); event <= 2048; event++ {
				report(event*500, 0)
			}

			return &servicepb.AnalyzeAccountsResponse{TotalAccounts: 1_024_000}, nil
		},
	)
	stream := newFakeServerStream[servicepb.AnalyzeAccountsEvent](t)

	err := (&BucketServiceServerImpl{ctrl: controller}).AnalyzeAccounts(&servicepb.AnalyzeAccountsRequest{Ledger: "main"}, stream)
	require.NoError(t, err)
	require.Len(t, stream.sent, 13) // 12 power-of-two callbacks and one result.
	require.Equal(t, uint64(1_024_000), stream.sent[len(stream.sent)-1].GetResult().GetTotalAccounts())
}

func TestAnalyzeAccountsSendFailureCancelsTheControllerAndSendsNoResult(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("stream closed")
	controller := NewMockController(gomock.NewController(t))
	controller.EXPECT().AnalyzeAccounts(gomock.Any(), "main", uint32(0), gomock.Any()).DoAndReturn(
		func(ctx context.Context, _ string, _ uint32, report func(uint64, uint64)) (*servicepb.AnalyzeAccountsResponse, error) {
			report(500, 0)
			<-ctx.Done()

			return nil, ctx.Err()
		},
	)
	stream := newFakeServerStream[servicepb.AnalyzeAccountsEvent](t)
	stream.sendStop, stream.sendErr = 1, wantErr

	err := (&BucketServiceServerImpl{ctrl: controller}).AnalyzeAccounts(&servicepb.AnalyzeAccountsRequest{Ledger: "main"}, stream)
	require.ErrorIs(t, err, wantErr)
	require.Len(t, stream.sent, 1)
	require.NotNil(t, stream.sent[0].GetProgress())
}

func TestAnalyzeTransactionsSamplesAcrossBothPasses(t *testing.T) {
	t.Parallel()

	controller := NewMockController(gomock.NewController(t))
	controller.EXPECT().AnalyzeTransactions(gomock.Any(), "main", uint32(0), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ string, _ uint32, report func(uint64, uint64)) (*servicepb.AnalyzeTransactionsResponse, error) {
			for event := uint64(1); event <= 2048; event++ {
				total := uint64(0)
				if event > 1024 {
					total = 1_024_000
				}
				report(event*500, total)
			}

			return &servicepb.AnalyzeTransactionsResponse{TotalTransactions: 512_000}, nil
		},
	)
	stream := newFakeServerStream[servicepb.AnalyzeTransactionsEvent](t)

	err := (&BucketServiceServerImpl{ctrl: controller}).AnalyzeTransactions(&servicepb.AnalyzeTransactionsRequest{Ledger: "main"}, stream)
	require.NoError(t, err)
	require.Len(t, stream.sent, 13)
	require.Zero(t, stream.sent[0].GetProgress().GetTotal())
	require.Equal(t, uint64(1_024_000), stream.sent[len(stream.sent)-2].GetProgress().GetTotal())
	require.Equal(t, uint64(512_000), stream.sent[len(stream.sent)-1].GetResult().GetTotalTransactions())
}

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
