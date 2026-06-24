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
	internal.RunDriver("parallel_driver_indexes", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		metadataKey := fmt.Sprintf("idx-key-%d", internal.Rand().Uint64()%50)
		details := internal.Details{"ledger": ledger, "metadataKey": metadataKey}

		// The metadata schema field must exist before its index — declare it
		// (idempotent / harmless if already declared).
		if _, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_SetMetadataFieldType{
				SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
					Ledger:     ledger,
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        metadataKey,
					Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
				},
			},
		})); err != nil {
			st, _ := status.FromError(err)
			// AlreadyExists means the field is declared — fall through to
			// CreateIndex. Any other failure (including a transient one that may
			// not have committed) leaves the field undeclared, so we must NOT
			// attempt CreateIndex — it would legitimately fail "field not declared".
			if st.Code() != codes.AlreadyExists {
				if !internal.IsTransient(err) {
					assert.Unreachable("SetMetadataFieldType returned unexpected error", details.With(internal.Details{"error": err}))
				}

				return
			}
		}

		indexID := &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
			Target: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:    metadataKey,
		}}}

		// Create the account metadata index.
		_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_CreateIndex{
				CreateIndex: &servicepb.CreateIndexRequest{
					Ledger: ledger,
					Id:     indexID,
				},
			},
		}))

		if err != nil {
			if internal.IsTransient(err) {
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
			if !internal.IsTransient(err) {
				assert.Unreachable("GetIndexStatus returned unexpected error", details.With(internal.Details{"error": err}))
			}

			return
		}

		assert.AlwaysOrUnreachable(statusResp != nil, "GetIndexStatus should return a response", details)

		// Drop the index.
		_, err = client.Apply(ctx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
			Type: &servicepb.Request_DropIndex{
				DropIndex: &servicepb.DropIndexRequest{
					Ledger: ledger,
					Id:     indexID,
				},
			},
		}))

		if err != nil && !internal.IsTransient(err) {
			st, _ := status.FromError(err)
			if st.Code() != codes.NotFound {
				assert.Unreachable("DropIndex returned unexpected error", details.With(internal.Details{"error": err}))
			}
		}
	})
}
