package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

)

func main() {
	internal.RunDriver("parallel_driver_update_query", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		queryName := fmt.Sprintf("upd-q-%d", r.Uint64())

		details := internal.Details{"ledger": ledger, "queryName": queryName}

		// 1. Create a prepared query filtering by "users:" prefix.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
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
			}},
		})
		if err != nil && !internal.IsTransient(err) {
			st, _ := status.FromError(err)
			if st.Code() != codes.AlreadyExists {
				return
			}
		}

		// 2. Update the query filter to "world" prefix.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_UpdatePreparedQuery{
					UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{
						Ledger: ledger,
						Name:   queryName,
						Filter: &commonpb.QueryFilter{
							Filter: &commonpb.QueryFilter_Address{
								Address: &commonpb.AddressMatch{
									Match: &commonpb.AddressMatch_HardcodedPrefix{
										HardcodedPrefix: "world",
									},
								},
							},
						},
					},
				},
			}},
		})

		assert.Sometimes(err == nil || internal.IsTransient(err),
			"should be able to update prepared query",
			details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		// 3. Execute the updated query — should only return accounts matching "world".
		execResp, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
			Ledger:    ledger,
			QueryName: queryName,
			PageSize:  100,
		})
		if err != nil {
			return
		}

		assert.AlwaysOrUnreachable(execResp != nil, "updated query should return a response", details)

		// 4. Cleanup.
		_, _ = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_DeletePreparedQuery{
					DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{
						Ledger: ledger,
						Name:   queryName,
					},
				},
			}},
		})
	})
}
