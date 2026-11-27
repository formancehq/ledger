package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"

	internalAuth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

type blockedLogSinkStream struct {
	servicepb.BucketService_SubscribeLedgerLogsServer

	started chan struct{}
	release chan struct{}
}

func (stream *blockedLogSinkStream) Send(*servicepb.SubscribeLedgerLogsResponse) error {
	close(stream.started)
	<-stream.release

	return nil
}

func TestLogSinkTokenExpiryInterruptsBlockedDelivery(t *testing.T) {
	t.Parallel()
	claims := &oidc.AccessTokenClaims{TokenClaims: oidc.TokenClaims{
		Expiration: oidc.Time(time.Now().Add(2 * time.Second).Unix()),
	}}
	ctx, cancel, expiry, err := logSinkAuthorizationContext(internalAuth.WithClaims(t.Context(), claims))
	require.NoError(t, err)
	defer cancel()
	require.Equal(t, claims.GetExpiration().Time, expiry)
	stream := &blockedLogSinkStream{started: make(chan struct{}), release: make(chan struct{})}
	defer close(stream.release)
	result := make(chan error, 1)
	go func() {
		result <- sendLogSinkResponse(ctx, stream, &servicepb.SubscribeLedgerLogsResponse{})
	}()
	select {
	case <-stream.started:
	case <-t.Context().Done():
		t.Fatal("send did not start")
	}
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.DeadlineExceeded)
	case <-time.After(3 * time.Second):
		t.Fatal("send continued after token expiry")
	}
}

func TestLogSinkAuthorizationDeadlineFollowsTokenExpiry(t *testing.T) {
	t.Parallel()
	expiry := oidc.Time(time.Now().Add(24 * time.Hour).Unix())
	claims := &oidc.AccessTokenClaims{TokenClaims: oidc.TokenClaims{Expiration: expiry}}
	ctx, cancel, actualExpiry, err := logSinkAuthorizationContext(internalAuth.WithClaims(t.Context(), claims))
	require.NoError(t, err)
	defer cancel()
	deadline, ok := ctx.Deadline()
	require.True(t, ok)
	require.True(t, time.Unix(int64(expiry), 0).Equal(actualExpiry))
	require.True(t, actualExpiry.Equal(deadline))
}

func TestReadLogSinkPageContinuesAfterLargeHistoricalLog(t *testing.T) {
	t.Parallel()
	controller := NewMockController(gomock.NewController(t))
	large := testLedgerLog(1)
	large.ResponseSignature = &signaturepb.SignedLog{Payload: make([]byte, maxLogSinkPageBytes+1)}
	next := testLedgerLog(2)
	controller.EXPECT().ListLogs(gomock.Any(), "ledger", uint64(0), uint32(2), nil, false).
		Return(cursor.NewSliceCursor([]*commonpb.Log{large, next}), nil)
	controller.EXPECT().ListLogs(gomock.Any(), "ledger", uint64(1), uint32(2), nil, false).
		Return(cursor.NewSliceCursor([]*commonpb.Log{next}), nil)
	impl := &BucketServiceServerImpl{ctrl: controller}
	first, err := impl.readLogSinkPage(context.Background(), "ledger", 0, 2)
	require.NoError(t, err)
	require.Equal(t, []*commonpb.Log{large}, first)
	second, err := impl.readLogSinkPage(context.Background(), "ledger", 1, 2)
	require.NoError(t, err)
	require.Equal(t, []*commonpb.Log{next}, second)
}

func TestSubscribeWrapperFitsListLogsBoundary(t *testing.T) {
	t.Parallel()
	log := testLedgerLog(1)
	log.ResponseSignature = &signaturepb.SignedLog{Payload: make([]byte, 64<<20)}
	for log.SizeVT() > 64<<20 {
		log.ResponseSignature.Payload = log.GetResponseSignature().GetPayload()[:len(log.GetResponseSignature().GetPayload())-1]
	}
	require.Equal(t, 64<<20, log.SizeVT())
	wrapped := &servicepb.SubscribeLedgerLogsResponse{Kind: &servicepb.SubscribeLedgerLogsResponse_Log{Log: log}}
	require.LessOrEqual(t, wrapped.SizeVT(), serviceMaxMsgSize)
}

func TestSubscribeLedgerLogsUsesLedgerReadScope(t *testing.T) {
	t.Parallel()
	policy, err := commonpb.RPCAuthPolicyForMethod(servicepb.BucketService_SubscribeLedgerLogs_FullMethodName)
	require.NoError(t, err)
	require.Equal(t, commonpb.AuthScope_AUTH_SCOPE_LEDGER_READ, policy.GetFixedScope())
}

func TestLogSinkConnectionLimitsAndRelease(t *testing.T) {
	t.Parallel()
	impl := &BucketServiceServerImpl{}
	require.NoError(t, impl.claimLogSink("first", "principal"))
	require.NoError(t, impl.claimLogSink("second", "principal"))
	require.Equal(t, codes.AlreadyExists, status.Code(impl.claimLogSink("first", "other")))
	require.Equal(t, codes.ResourceExhausted, status.Code(impl.claimLogSink("third", "principal")))
	impl.releaseLogSink("first")
	require.NoError(t, impl.claimLogSink("third", "principal"))
}

func TestLogSinkPendingOpenCountsAgainstGlobalLimit(t *testing.T) {
	t.Parallel()
	impl := &BucketServiceServerImpl{}
	for range maxActiveLogSinkStreams {
		require.NoError(t, impl.reserveLogSinkOpen())
	}
	require.Equal(t, codes.ResourceExhausted, status.Code(impl.reserveLogSinkOpen()))
	require.NoError(t, impl.promoteLogSinkOpen("first", "principal"))
	require.Equal(t, codes.ResourceExhausted, status.Code(impl.reserveLogSinkOpen()))
	impl.releaseLogSink("first")
	require.NoError(t, impl.reserveLogSinkOpen())
}

func TestLogSinkOwnerMatchesAuthenticatedPrincipal(t *testing.T) {
	t.Parallel()
	ctx := internalAuth.WithClaims(context.Background(), &oidc.AccessTokenClaims{TokenClaims: oidc.TokenClaims{
		Issuer: "https://idp.example.com", Subject: "sink-owner",
	}})
	owner := &commonpb.CallerIdentity{Subject: "sink-owner", Source: &commonpb.CallerIdentity_Issuer{Issuer: "https://idp.example.com"}}
	require.True(t, logSinkOwnerMatches(ctx, owner))
	require.False(t, logSinkOwnerMatches(ctx, &commonpb.CallerIdentity{Subject: "other", Source: &commonpb.CallerIdentity_Issuer{Issuer: "https://idp.example.com"}}))
	require.False(t, logSinkOwnerMatches(ctx, &commonpb.CallerIdentity{Subject: "sink-owner", Source: &commonpb.CallerIdentity_Issuer{Issuer: "https://other.example.com"}}))
	require.False(t, logSinkOwnerMatches(context.Background(), owner))
	static := internalAuth.WithKeyID(ctx, "key-1")
	require.True(t, logSinkOwnerMatches(static, &commonpb.CallerIdentity{Subject: "sink-owner", Source: &commonpb.CallerIdentity_KeyId{KeyId: "key-1"}}))
	require.False(t, logSinkOwnerMatches(static, owner), "a static credential must not borrow an OIDC issuer")
}

func testLedgerLog(id uint64) *commonpb.Log {
	return &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Id: id},
	}}}}
}
