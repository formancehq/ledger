package main

import (
	"context"
	"fmt"
	"math"
	"math/big"
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/drivertest"
)

// Capture actual outgoing requests while delegating all behavior to the real server.
type recordingClient struct {
	servicepb.BucketServiceClient
	requests []*servicepb.ApplyRequest
	errors   []error
}

func (c *recordingClient) Apply(ctx context.Context, req *servicepb.ApplyRequest, opts ...grpc.CallOption) (*servicepb.ApplyResponse, error) {
	c.requests = append(c.requests, proto.Clone(req).(*servicepb.ApplyRequest))
	resp, err := c.BucketServiceClient.Apply(ctx, req, opts...)
	c.errors = append(c.errors, err)
	return resp, err
}

func TestConflictVariants(t *testing.T) {
	if !assert.Enabled {
		t.Skip("requires enable_antithesis_sdk")
	}
	drivertest.CheckEmissions(t, func() {
		ctx, client := drivertest.StartServer(t)
		amounts := [][2]uint64{{100, 999}, {0, 0}, {1, 1}, {math.MaxUint64, math.MaxUint64}, {0, math.MaxUint64}, {math.MaxUint64, 0}, {1, 1<<63 | 1}}
		for _, amountOnly := range []bool{false, true} {
			for i, amounts := range amounts {
				ledger := fmt.Sprintf("conflict-%t-%d", amountOnly, i)
				_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("seed-"+ledger, actions.CreateLedgerAction(ledger, nil)))
				require.NoError(t, err)
				recording := &recordingClient{BucketServiceClient: client}
				runConflict(ctx, recording, ledger, "key-"+ledger, amounts[0], amounts[1], amountOnly)
				require.Len(t, recording.requests, 2, "scenario must execute exactly once")
				require.NoError(t, recording.errors[0])
				require.True(t, internal.HasErrorReason(recording.errors[1], domain.ErrReasonIdempotencyKeyConflict))
				first, second := recording.requests[0], recording.requests[1]
				posting := second.GetUnsigned().GetRequests()[0].GetApply().GetAction().GetCreateTransaction().GetPostings()[0]
				if amountOnly {
					require.False(t, proto.Equal(commonpb.NewUint256FromUint64(amounts[0]), posting.Amount))
				} else {
					require.Equal(t, "users:1", posting.Destination)
					require.Equal(t, "EUR/2", posting.Asset)
					require.True(t, proto.Equal(commonpb.NewUint256FromUint64(amounts[1]), posting.Amount))
					posting.Destination = "users:0"
					posting.Asset = "USD/2"
				}
				// After removing the intended differences, every request field (including
				// key, ledger, force, source and other payload fields) must be identical.
				posting.Amount = commonpb.NewUint256FromUint64(amounts[0])
				require.True(t, proto.Equal(first, second), "unexpected payload difference")
				ids, err := internal.ReadOracleTransactions(ctx, client, ledger, nil)
				require.NoError(t, err)
				require.Len(t, ids, 1, "conflict must not create a second transaction")
				account, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{Ledger: ledger, Address: "users:0"})
				require.NoError(t, err)
				volume := account.FindVolume("USD/2", "")
				require.NotNil(t, volume)
				input, err := volume.GetInput().ToBigInt()
				require.NoError(t, err)
				require.Equal(t, new(big.Int).SetUint64(amounts[0]), input, "conflict must not repeat credits")
			}
		}
	}, func(records []drivertest.Assertion) {
		hits := map[string]int{}
		for _, record := range records {
			if !record.Hit {
				continue
			}
			if record.Type == "always" || record.Type == "reachability" {
				require.True(t, record.Condition, "unexpected assertion: %+v", record)
			}
			hits[record.Message]++
		}
		// The SDK deduplicates repeated successful hits at the same call site.
		// Per-case error, payload and state checks above cover every invocation.
		require.Positive(t, hits["mismatched idempotency key should return IDEMPOTENCY_KEY_CONFLICT"])
		require.Positive(t, hits["idempotency conflict path exercised"])
	})
}
