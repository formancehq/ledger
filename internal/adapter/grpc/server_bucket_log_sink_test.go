package grpc

import (
	"context"
	"crypto/sha256"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"

	internalAuth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type blockedLogSinkStream struct {
	servicepb.BucketService_SubscribeLogsServer

	started chan struct{}
	release chan struct{}
}

func (stream *blockedLogSinkStream) Send(*servicepb.SubscribeLogsResponse) error {
	close(stream.started)
	<-stream.release

	return nil
}

func TestLogSinkTokenExpiryInterruptsBlockedDelivery(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		claims := &oidc.AccessTokenClaims{TokenClaims: oidc.TokenClaims{
			Expiration: oidc.Time(time.Now().Add(2 * time.Second).Unix()),
		}}
		ctx, cancel, expiry, err := logSinkAuthorizationContext(internalAuth.WithClaims(context.Background(), claims))
		require.NoError(t, err)
		defer cancel()
		require.Equal(t, claims.GetExpiration().Time, expiry)
		stream := &blockedLogSinkStream{started: make(chan struct{}), release: make(chan struct{})}
		defer func() {
			close(stream.release)
			synctest.Wait()
		}()
		result := make(chan error, 1)
		go func() {
			result <- sendLogSinkResponse(ctx, stream, &servicepb.SubscribeLogsResponse{})
		}()
		<-stream.started
		synctest.Wait()
		require.NoError(t, ctx.Err(), "delivery must be blocked before token expiry")
		<-time.After(time.Until(expiry))
		require.ErrorIs(t, <-result, context.DeadlineExceeded)
	})
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
	large := testLedgerLog(1)
	large.ResponseSignature = &signaturepb.SignedLog{Payload: make([]byte, maxLogSinkPageBytes+1)}
	next := testLedgerLog(2)
	store := newLogSinkTestStore(t, large, next)
	impl := &BucketServiceServerImpl{store: store}
	first, err := impl.readLogSinkPage(t.Context(), 0, 2)
	require.NoError(t, err)
	require.Equal(t, []*commonpb.Log{large}, first)
	second, err := impl.readLogSinkPage(t.Context(), 1, 2)
	require.NoError(t, err)
	require.Equal(t, []*commonpb.Log{next}, second)
}

func TestReadLogSinkPageDefersLogThatExceedsNonemptyPageBudget(t *testing.T) {
	t.Parallel()
	firstLog := testLedgerLog(1)
	firstLog.ResponseSignature = &signaturepb.SignedLog{Payload: make([]byte, 7<<20)}
	secondLog := testLedgerLog(2)
	secondLog.ResponseSignature = &signaturepb.SignedLog{Payload: make([]byte, 7<<20)}
	require.Less(t, firstLog.SizeVT(), maxLogSinkPageBytes)
	require.Greater(t, firstLog.SizeVT()+secondLog.SizeVT(), maxLogSinkPageBytes)
	store := newLogSinkTestStore(t, firstLog, secondLog)
	impl := &BucketServiceServerImpl{store: store}
	page, err := impl.readLogSinkPage(t.Context(), 0, 2)
	require.NoError(t, err)
	require.Equal(t, []*commonpb.Log{firstLog}, page)
	page, err = impl.readLogSinkPage(t.Context(), 1, 2)
	require.NoError(t, err)
	require.Equal(t, []*commonpb.Log{secondLog}, page)
}

func TestReadLogSinkPageIncludesOtherLedgersAndSystemLogs(t *testing.T) {
	t.Parallel()
	first := testLedgerLog(1)
	second := testLedgerLog(2)
	second.GetPayload().GetApply().LedgerName = "other-ledger"
	system := &commonpb.Log{Sequence: 3, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_RemovedEventsSink{
		RemovedEventsSink: &commonpb.RemovedEventsSinkLog{Name: "old-sink"},
	}}}
	impl := &BucketServiceServerImpl{store: newLogSinkTestStore(t, first, second, system)}
	page, err := impl.readLogSinkPage(t.Context(), 0, 3)
	require.NoError(t, err)
	require.Equal(t, []*commonpb.Log{first, second, system}, page)
	page, err = impl.readLogSinkPage(t.Context(), 1, 3)
	require.NoError(t, err)
	require.Equal(t, []*commonpb.Log{second, system}, page)
}

func TestSubscribeWrapperFitsListLogsBoundary(t *testing.T) {
	t.Parallel()
	log := testLedgerLog(1)
	log.ResponseSignature = &signaturepb.SignedLog{Payload: make([]byte, 64<<20)}
	for log.SizeVT() > 64<<20 {
		log.ResponseSignature.Payload = log.GetResponseSignature().GetPayload()[:len(log.GetResponseSignature().GetPayload())-1]
	}
	require.Equal(t, 64<<20, log.SizeVT())
	wrapped := &servicepb.SubscribeLogsResponse{Kind: &servicepb.SubscribeLogsResponse_Log{Log: log}}
	require.LessOrEqual(t, wrapped.SizeVT(), serviceMaxMsgSize)
}

func TestSubscribeLogsUsesCredentialResolver(t *testing.T) {
	t.Parallel()
	policy, err := commonpb.RPCAuthPolicyForMethod(servicepb.BucketService_SubscribeLogs_FullMethodName)
	require.NoError(t, err)
	require.Equal(t, commonpb.DynamicAuthResolver_DYNAMIC_AUTH_RESOLVER_SUBSCRIBE_LOGS, policy.GetDynamicResolver())
}

func TestLogSinkSecretVerifier(t *testing.T) {
	t.Parallel()
	secret := "a-client-generated-random-secret-with-at-least-32-bytes"
	digest := sha256.Sum256([]byte(secret))
	require.True(t, logSinkSecretMatches(secret, digest[:]))
	require.False(t, logSinkSecretMatches(secret+"x", digest[:]))
	require.False(t, logSinkSecretMatches(secret, digest[:31]))
	ctx := context.WithValue(t.Context(), logSinkSecretKey{}, secret)
	authorized, cancel, expiry, err := logSinkAuthorizationContext(ctx)
	require.NoError(t, err)
	defer cancel()
	require.True(t, expiry.IsZero())
	require.False(t, authorized == nil)
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
	return &commonpb.Log{Sequence: id, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
		LedgerName: "ledger", Log: &commonpb.LedgerLog{Id: id},
	}}}}
}

func newLogSinkTestStore(t *testing.T, logs ...*commonpb.Log) *dal.Store {
	t.Helper()
	store, err := dal.NewStore(t.TempDir(), noopLogger{}, noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })
	batch := store.OpenWriteSession()
	for _, log := range logs {
		key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(log.GetSequence()).Build()
		require.NoError(t, batch.SetProto(key, log))
	}
	require.NoError(t, batch.Commit())

	return store
}
