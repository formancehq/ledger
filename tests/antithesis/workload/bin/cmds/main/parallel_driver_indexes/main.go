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
	internal.RunDriver("parallel_driver_indexes", func(ctx context.Context, client ledgerpb.BucketServiceClient, ledger string) {
		metadataKey := fmt.Sprintf("idx-key-%d", internal.Rand().Uint64()%50)
		details := internal.Details{"ledger": ledger, "metadataKey": metadataKey}

		// The metadata schema field must exist before its index — declare it
		// (idempotent / harmless if already declared).
		if _, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_SetMetadataFieldType{
				SetMetadataFieldType: &ledgerpb.SetMetadataFieldTypeRequest{
					Ledger:     ledger,
					TargetType: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        metadataKey,
					Type:       ledgerpb.MetadataType_METADATA_TYPE_STRING,
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

		indexID := &ledgerpb.IndexID{Kind: &ledgerpb.IndexID_Metadata{Metadata: &ledgerpb.MetadataIndexID{
			Target: ledgerpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:    metadataKey,
		}}}

		// Create the account metadata index.
		_, err := client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_CreateIndex{
				CreateIndex: &ledgerpb.CreateIndexRequest{
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
		statusResp, err := client.GetIndexStatus(ctx, &ledgerpb.GetIndexStatusRequest{})
		if err != nil {
			if !internal.IsTransient(err) {
				assert.Unreachable("GetIndexStatus returned unexpected error", details.With(internal.Details{"error": err}))
			}

			return
		}

		assert.AlwaysOrUnreachable(statusResp != nil, "GetIndexStatus should return a response", details)

		// Drop the index.
		_, err = client.Apply(ctx, ledgerpb.UnsignedApplyRequest("", &ledgerpb.Request{
			Type: &ledgerpb.Request_DropIndex{
				DropIndex: &ledgerpb.DropIndexRequest{
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
