package grpc

import (
	"crypto/sha256"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

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

func TestLogSinkPendingOpensDoNotConsumeActiveCapacity(t *testing.T) {
	t.Parallel()
	impl := &BucketServiceServerImpl{}
	for range maxPendingLogSinkOpens {
		require.NoError(t, impl.reserveLogSinkOpen())
	}
	require.Equal(t, codes.ResourceExhausted, status.Code(impl.reserveLogSinkOpen()))
	for i := range maxActiveLogSinkStreams {
		name := fmt.Sprintf("sink-%d", i)
		require.NoError(t, impl.claimLogSink(name, "sink:"+name), "pending opens must not consume active capacity")
	}
	require.Equal(t, codes.ResourceExhausted, status.Code(impl.promoteLogSinkOpen("next", "sink:next")))
	impl.releaseLogSink("sink-0")
	require.NoError(t, impl.promoteLogSinkOpen("next", "sink:next"))
	for range maxPendingLogSinkOpens {
		require.NoError(t, impl.reserveLogSinkOpen())
	}
	require.Equal(t, codes.ResourceExhausted, status.Code(impl.reserveLogSinkOpen()))
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
