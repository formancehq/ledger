package internal

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
)

type sentinelCommitServer struct {
	servicepb.UnimplementedBucketServiceServer

	requests chan *servicepb.ApplyRequest
	code     codes.Code
	missing  bool
}

func (s *sentinelCommitServer) Apply(_ context.Context, req *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	s.requests <- req
	if s.code != codes.OK {
		return nil, status.Error(s.code, "sentinel commit fixture")
	}
	if s.missing {
		return &servicepb.ApplyResponse{}, nil
	}

	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{LedgerName: "sentinel-ledger", Log: &commonpb.LedgerLog{
				Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{
						Id:        42,
						Reference: req.GetUnsigned().GetRequests()[0].GetApply().GetAction().GetCreateTransaction().GetReference(),
					}},
				}},
			}},
		}},
	}}}, nil
}

func TestPreCommitGeneratedSentinel(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"success", "rpc-error", "missing-transaction"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := &sentinelCommitServer{requests: make(chan *servicepb.ApplyRequest, 2), missing: name == "missing-transaction"}
			if name == "rpc-error" {
				server.code = codes.Unavailable
			}
			client := sdktest.Client(t, server)
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			sentinel, err := PreCommitSentinel(ctx, client, "sentinel-ledger")
			if name == "success" {
				require.NoError(t, err)
				require.Equal(t, uint64(42), sentinel.TxID)
				require.Equal(t, "sentinel-ledger", sentinel.Ledger)
			} else {
				require.Nil(t, sentinel)
				if name == "rpc-error" {
					require.Equal(t, codes.Unavailable, status.Code(err))
				} else {
					require.EqualError(t, err, "apply returned no CreatedTransaction")
				}
			}
			require.Len(t, server.requests, 1, "generation must never replay the commit RPC")
			req := <-server.requests
			require.Len(t, req.GetUnsigned().GetRequests(), 1)
			apply := req.GetUnsigned().GetRequests()[0].GetApply()
			require.Equal(t, "sentinel-ledger", apply.GetLedger())
			payload := apply.GetAction().GetCreateTransaction()
			require.True(t, payload.GetForce())
			require.True(t, strings.HasPrefix(payload.GetReference(), "sentinel-"))
			if sentinel != nil {
				require.Equal(t, payload.GetReference(), sentinel.Reference)
			}
			require.Len(t, payload.GetPostings(), 1)
			posting := payload.GetPostings()[0]
			require.Equal(t, "world", posting.GetSource())
			require.True(t, strings.HasPrefix(posting.GetDestination(), "sentinel:"))
			require.Equal(t, "COIN", posting.GetAsset())
			require.NotNil(t, posting.GetAmount())
			require.True(t, posting.GetAmount().ToBigInt().IsUint64())
		})
	}
}

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
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
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
				sentinel := newSentinel("sdk-sentinel", &commonpb.Transaction{Id: 42})
				sentinel.Verify(ctx, client, "after-operation")
				require.EqualValues(t, 1, server.calls.Load())
			})
			if events == nil {
				return
			}
			if code != codes.OK {
				sdktest.Absent(t, events, sentinelContentProperty)
			}
			switch code {
			case codes.OK:
				require.True(t, sdktest.Find(t, events, sentinelContentProperty).Condition)
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
