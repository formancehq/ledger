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
	internal.RunDriver("parallel_driver_update_query", func(ctx context.Context, client ledgerpb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		queryName := fmt.Sprintf("upd-q-%d", r.Uint64())

		details := internal.Details{"ledger": ledger, "queryName": queryName}

		// 1. Create a prepared query filtering by "users:" prefix.
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
			if internal.IsTolerated(err) {
				return
			}

			st, _ := status.FromError(err)
			if st.Code() != codes.AlreadyExists {
				assert.Unreachable("prepared query creation returned unexpected error",
					details.With(internal.Details{"error": err}))

				return
			}
		}

		// 2. Update the query filter to "world" prefix.
		_, err = client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_UpdatePreparedQuery{
				UpdatePreparedQuery: &ledgerpb.UpdatePreparedQueryRequest{
					Ledger: ledger,
					Name:   queryName,
					Filter: &ledgerpb.QueryFilter{
						Filter: &ledgerpb.QueryFilter_Address{
							Address: &ledgerpb.AddressMatch{
								Match: &ledgerpb.AddressMatch_HardcodedPrefix{
									HardcodedPrefix: "world",
								},
							},
						},
					},
				},
			},
		}))

		assert.Sometimes(internal.IsTolerated(err),
			"should be able to update prepared query",
			details.With(internal.Details{"error": err}))
		if err != nil {
			return
		}

		// 3. Execute the updated query — should only return accounts matching "world".
		execResp, err := client.ExecutePreparedQuery(ctx, &ledgerpb.ExecutePreparedQueryRequest{
			Ledger:    ledger,
			QueryName: queryName,
			PageSize:  100,
		})
		if err != nil {
			internal.LogCleanupError(fmt.Sprintf("execute prepared query %q", queryName), err)

			return
		}

		assert.AlwaysOrUnreachable(execResp != nil, "updated query should return a response", details)

		// 4. Cleanup.
		if _, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_DeletePreparedQuery{
				DeletePreparedQuery: &ledgerpb.DeletePreparedQueryRequest{
					Ledger: ledger,
					Name:   queryName,
				},
			},
		})); err != nil {
			internal.LogCleanupError(fmt.Sprintf("delete prepared query %q", queryName), err)
		}
	})
}
