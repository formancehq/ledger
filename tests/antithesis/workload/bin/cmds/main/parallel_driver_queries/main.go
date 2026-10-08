package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

func main() {
	internal.RunDriver("parallel_driver_queries", func(ctx context.Context, client ledgerpb.BucketServiceClient, ledger string) {
		queryName := fmt.Sprintf("q-%d", internal.Rand().Uint64()%100)
		details := internal.Details{"ledger": ledger, "queryName": queryName}

		_, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_CreatePreparedQuery{
				CreatePreparedQuery: &ledgerpb.CreatePreparedQueryRequest{
					Ledger: ledger,

					Query: &ledgerpb.PreparedQuery{
						Name:   queryName,
						Target: ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
						Filter: &ledgerpb.QueryFilter{
							Filter: &ledgerpb.QueryFilter_Address{
								Address: &ledgerpb.AddressMatch{
									Match: &ledgerpb.AddressMatch_HardcodedPrefix{
										HardcodedPrefix: "users:",
									},
								},
							},
						},
					},
				},
			},
		}))

		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			st, _ := status.FromError(err)
			if st.Code() != codes.AlreadyExists {
				assert.Unreachable("prepared query creation returned unexpected error", details.With(internal.Details{"error": err}))
			}
		}

		execResp, err := client.ExecutePreparedQuery(ctx, &ledgerpb.ExecutePreparedQueryRequest{
			Ledger:    ledger,
			QueryName: queryName,
			PageSize:  10,
		})

		// Names come from a small shared pool, so another worker can delete this
		// query between our create and execute — an expected NotFound race (the
		// delete path below tolerates the mirror case), not a failure.
		if internal.IsNotFound(err) {
			return
		}

		assert.Sometimes(internal.IsTolerated(err), "should be able to execute prepared query", details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		assert.AlwaysOrUnreachable(execResp != nil, "prepared query should return a response", details)

		_, err = client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_DeletePreparedQuery{
				DeletePreparedQuery: &ledgerpb.DeletePreparedQueryRequest{
					Ledger: ledger,
					Name:   queryName,
				},
			},
		}))

		if err != nil && !internal.IsTransient(err) {
			st, _ := status.FromError(err)
			if st.Code() != codes.NotFound {
				assert.Unreachable("prepared query deletion returned unexpected error", details.With(internal.Details{"error": err}))
			}
		}
	})
}
