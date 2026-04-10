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
	internal.RunDriver("parallel_driver_indexes", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		metadataKey := fmt.Sprintf("idx-key-%d", internal.Rand().Uint64()%50)
		details := internal.Details{"ledger": ledger, "metadataKey": metadataKey}

		// Create an account metadata index.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_CreateIndex{
					CreateIndex: &servicepb.CreateIndexRequest{
						Ledger: ledger,
						Index: &servicepb.CreateIndexRequest_Account{
							Account: &commonpb.AccountIndex{
								Kind: &commonpb.AccountIndex_MetadataKey{
									MetadataKey: metadataKey,
								},
							},
						},
					},
				},
			}},
		})

		if err != nil {
			if internal.IsUnavailable(err) {
				return
			}

			st, _ := status.FromError(err)
			if st.Code() != codes.AlreadyExists {
				assert.Unreachable("CreateIndex returned unexpected error", details.With(internal.Details{"error": err}))
			}
		}

		// Check index status.
		statusResp, err := client.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{})
		if err != nil {
			if !internal.IsUnavailable(err) {
				assert.Unreachable("GetIndexStatus returned unexpected error", details.With(internal.Details{"error": err}))
			}

			return
		}

		assert.AlwaysOrUnreachable(statusResp != nil, "GetIndexStatus should return a response", details)

		// Drop the index.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_DropIndex{
					DropIndex: &servicepb.DropIndexRequest{
						Ledger: ledger,
						Index: &servicepb.DropIndexRequest_Account{
							Account: &commonpb.AccountIndex{
								Kind: &commonpb.AccountIndex_MetadataKey{
									MetadataKey: metadataKey,
								},
							},
						},
					},
				},
			}},
		})

		if err != nil && !internal.IsUnavailable(err) {
			st, _ := status.FromError(err)
			if st.Code() != codes.NotFound {
				assert.Unreachable("DropIndex returned unexpected error", details.With(internal.Details{"error": err}))
			}
		}
	})
}
