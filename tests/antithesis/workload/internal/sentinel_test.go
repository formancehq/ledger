package internal

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
)

type sentinelSDKServer struct {
	servicepb.UnimplementedBucketServiceServer
	code  codes.Code
	calls atomic.Int32
}

func (s *sentinelSDKServer) GetTransaction(_ context.Context, request *servicepb.GetTransactionRequest) (*servicepb.GetTransactionResponse, error) {
	s.calls.Add(1)
	if request.GetLedger() != "sdk-sentinel" || request.GetTransactionId() != 42 {
		return nil, status.Error(codes.InvalidArgument, "wrong sentinel identity")
	}
	if s.code != codes.OK {
		return nil, status.Error(s.code, "sentinel read fixture")
	}
	return &servicepb.GetTransactionResponse{Transaction: &commonpb.Transaction{Id: 42}}, nil
}

func TestSentinelVerifySDK(t *testing.T) {
	t.Parallel()
	const survival = "committed sentinel transaction must survive operational events"
	const success = "sentinel transaction read-after-write succeeded"
	for _, code := range []codes.Code{codes.OK, codes.NotFound, codes.Unavailable, codes.DeadlineExceeded, codes.FailedPrecondition, codes.Unknown} {
		t.Run(code.String(), func(t *testing.T) {
			t.Parallel()
			events := sdktest.Capture(t, func() {
				server := &sentinelSDKServer{code: code}
				client := sdktest.Client(t, server, grpc.WithChainUnaryInterceptor(classifyUnaryInterceptor()))
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				sentinel := Sentinel{Ledger: "sdk-sentinel", Reference: "committed-reference", TxID: 42}
				sentinel.Verify(ctx, client, "after-operation")
				require.EqualValues(t, 1, server.calls.Load())
			})
			if events == nil {
				return
			}
			switch code {
			case codes.OK:
				event := sdktest.Find(t, events, success)
				require.Equal(t, "Reachable", event.DisplayType)
				require.True(t, event.Condition)
				require.True(t, event.MustHit)
				require.Equal(t, "sdk-sentinel", event.Details["ledger"])
				require.Equal(t, float64(42), event.Details["txId"])
				sdktest.Absent(t, events, survival)
			case codes.NotFound:
				event := sdktest.Find(t, events, survival)
				require.False(t, event.Condition)
				require.Contains(t, event.Details["error"], "NotFound")
				require.Equal(t, "after-operation", event.Details["label"])
				require.Equal(t, "Unreachable", event.DisplayType, "failure-only observation must not require a hit")
				require.False(t, event.MustHit)
				sdktest.Absent(t, events, success)
			case codes.Unavailable, codes.DeadlineExceeded:
				sdktest.Find(t, events, "sentinel verify hit a transient error")
				sdktest.Absent(t, events, success)
				sdktest.Absent(t, events, survival)
			case codes.FailedPrecondition:
				event := sdktest.Find(t, events, "every RPC error must be classified (workload predicate set complete)")
				require.True(t, event.Condition, "classified business errors remain observed")
				sdktest.Absent(t, events, "sentinel verify hit a transient error")
				sdktest.Absent(t, events, success)
				sdktest.Absent(t, events, survival)
			case codes.Unknown:
				event := sdktest.Find(t, events, "every RPC error must be classified (workload predicate set complete)")
				require.False(t, event.Condition, "unexpected errors remain findings")
				require.Equal(t, "Unknown", event.Details["code"])
				sdktest.Absent(t, events, success)
				sdktest.Absent(t, events, survival)
			}
		})
	}
}
