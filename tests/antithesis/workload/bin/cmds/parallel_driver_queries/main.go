package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	internal.RunDriver("parallel_driver_queries", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		queryName := fmt.Sprintf("q-%d", internal.Rand().Uint64()%100)
		details := internal.Details{"ledger": ledger, "queryName": queryName}

		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
				Type: &servicepb.Request_CreatePreparedQuery{
					CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
						Query: &commonpb.PreparedQuery{
							Name:   queryName,
							Ledger: ledger,
							Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
							Filter: &commonpb.QueryFilter{
								Filter: &commonpb.QueryFilter_Address{
									Address: &commonpb.AddressMatch{
										Match: &commonpb.AddressMatch_HardcodedPrefix{
											HardcodedPrefix: "users:",
										},
									},
								},
							},
						},
					},
				},
			}),
		})

		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			st, _ := status.FromError(err)
			if st.Code() != codes.AlreadyExists {
				assert.Unreachable("prepared query creation returned unexpected error", details.With(internal.Details{"error": err}))
			}
		}

		execResp, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
			Ledger:    ledger,
			QueryName: queryName,
			PageSize:  10,
		})

		assert.Sometimes(err == nil || internal.IsTransient(err), "should be able to execute prepared query", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		assert.AlwaysOrUnreachable(execResp != nil, "prepared query should return a response", details)

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
				Type: &servicepb.Request_DeletePreparedQuery{
					DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{
						Ledger: ledger,
						Name:   queryName,
					},
				},
			}),
		})

		if err != nil && !internal.IsTransient(err) {
			st, _ := status.FromError(err)
			if st.Code() != codes.NotFound {
				assert.Unreachable("prepared query deletion returned unexpected error", details.With(internal.Details{"error": err}))
			}
		}
	})
}
