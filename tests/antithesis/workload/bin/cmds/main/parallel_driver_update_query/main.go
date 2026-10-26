package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func main() {
	internal.RunGeneratedDriver("parallel_driver_update_query", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string, src *generation.Source) {
		r := internal.Rand()
		queryName := fmt.Sprintf("upd-q-%d", r.Uint64())
		prefixes := generation.Choice(src, [][2]string{
			{"users:", "world"},
			{"world", "users:"},
			{"users:0", "users:1"},
		})

		details := internal.Details{"ledger": ledger, "queryName": queryName}

		// 1. Create the initial prefix filter.
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_CreatePreparedQuery{
				CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
					Ledger: ledger,

					Query: &commonpb.PreparedQuery{
						Name:   queryName,
						Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
						Filter: &commonpb.QueryFilter{
							Filter: &commonpb.QueryFilter_Address{
								Address: &commonpb.AddressMatch{
									Match: &commonpb.AddressMatch_HardcodedPrefix{
										HardcodedPrefix: prefixes[0],
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

		// 2. Update to a distinct prefix, retaining the original users/world case.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_UpdatePreparedQuery{
				UpdatePreparedQuery: &servicepb.UpdatePreparedQueryRequest{
					Ledger: ledger,
					Name:   queryName,
					Filter: &commonpb.QueryFilter{
						Filter: &commonpb.QueryFilter_Address{
							Address: &commonpb.AddressMatch{
								Match: &commonpb.AddressMatch_HardcodedPrefix{
									HardcodedPrefix: prefixes[1],
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

		// 3. Execute the updated query.
		execResp, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
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
		if _, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_DeletePreparedQuery{
				DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{
					Ledger: ledger,
					Name:   queryName,
				},
			},
		})); err != nil {
			internal.LogCleanupError(fmt.Sprintf("delete prepared query %q", queryName), err)
		}
	})
}
