// parallel_driver_metadata_conversion exercises concurrent metadata type
// changes on the same key. While one goroutine writes string values, this
// driver declares the key as INT64, then BOOL, then removes the type — all
// while the existing parallel_driver_typed_metadata is also running.
//
// The key invariant: the schema status must always reflect the most recently
// committed type declaration, and GetAccount must never fail (data is never lost,
// only converted or nullified).
package main

import (
	"context"
	"fmt"
	"log"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// wellKnownKey is a metadata key shared across instances of this driver to
// maximize contention. Multiple parallel instances will race on declaring
// different types for the same key.
const wellKnownKey = "shared-conversion-target"

func main() {
	internal.RunDriver("parallel_driver_metadata_conversion", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		address := internal.GetRandomAddress()

		details := internal.Details{
			"ledger":  ledger,
			"key":     wellKnownKey,
			"account": address,
		}

		// 1. Write a string value for the well-known key.
		value := fmt.Sprintf("%d", r.Int63n(10000))

		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_Apply{
					Apply: &servicepb.LedgerApplyRequest{
						Ledger: ledger,
						Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
							AddMetadata: &commonpb.SaveMetadataCommand{
								Target: &commonpb.Target{
									Target: &commonpb.Target_Account{
										Account: &commonpb.TargetAccount{Addr: address},
									},
								},
								Metadata: commonpb.MetadataFromGoMap(map[string]string{
									wellKnownKey: value,
								}),
							},
						}},
					},
				},
			}},
		})
		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			log.Printf("metadata-conversion: write failed: %s", err)

			return
		}

		// 2. Randomly pick a type to declare (or remove).
		actions := []func(){
			func() { declareType(ctx, client, ledger, commonpb.MetadataType_METADATA_TYPE_INT64, details) },
			func() { declareType(ctx, client, ledger, commonpb.MetadataType_METADATA_TYPE_BOOL, details) },
			func() { declareType(ctx, client, ledger, commonpb.MetadataType_METADATA_TYPE_STRING, details) },
			func() { removeType(ctx, client, ledger, details) },
		}

		action := actions[r.Intn(len(actions))]
		action()

		// 3. Verify the account is still readable — data must never be lost.
		acct, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
			Ledger:  ledger,
			Address: address,
		})
		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			assert.Unreachable("account must be readable after type change",
				details.With(internal.Details{"error": err}))

			return
		}

		// The key must still exist in metadata (possibly converted or nullified).
		_, found := acct.GetMetadata()[wellKnownKey]
		assert.AlwaysOrUnreachable(found,
			"metadata key must survive type conversion",
			details.With(internal.Details{"value": value}),
		)

		// 4. Check schema status — must reflect a valid state.
		schema, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
			Ledger: ledger,
		})
		if err != nil {
			internal.LogCleanupError("get metadata schema status after conversion", err)
			return
		}

		if fieldStatus, ok := schema.GetAccountFields()[wellKnownKey]; ok {
			// If conversion is in progress, convertedKeys should not exceed totalKeys.
			converted := fieldStatus.GetConvertedKeys()
			total := fieldStatus.GetTotalKeys()

			if total > 0 {
				assert.AlwaysOrUnreachable(
					converted <= total,
					"converted keys must not exceed total keys during conversion",
					details.With(internal.Details{
						"convertedKeys": converted,
						"totalKeys":     total,
						"declaredType":  fieldStatus.GetDeclaredType().String(),
						"status":        fieldStatus.GetStatus().String(),
					}),
				)
			}
		}

		assert.Reachable("metadata conversion check passed", details)
	})
}

func declareType(ctx context.Context, client servicepb.BucketServiceClient, ledger string, metaType commonpb.MetadataType, details internal.Details) {
	details = details.With(internal.Details{"declareType": metaType.String()})

	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_SetMetadataFieldType{
				SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
					Ledger:     ledger,
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        wellKnownKey,
					Type:       metaType,
				},
			},
		}},
	})

	assert.AlwaysOrUnreachable(err == nil || internal.IsTransient(err),
		"declaring metadata type should not crash",
		details.With(internal.Details{"error": err}),
	)

	if err == nil {
		log.Printf("metadata-conversion: declared %s as %s on %s", wellKnownKey, metaType, ledger)
	}
}

func removeType(ctx context.Context, client servicepb.BucketServiceClient, ledger string, details internal.Details) {
	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Requests: []*servicepb.Request{{
			Type: &servicepb.Request_RemoveMetadataFieldType{
				RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
					Ledger:     ledger,
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        wellKnownKey,
				},
			},
		}},
	})

	assert.AlwaysOrUnreachable(err == nil || internal.IsTransient(err),
		"removing metadata type should not crash",
		details.With(internal.Details{"error": err}),
	)

	if err == nil {
		log.Printf("metadata-conversion: removed type for %s on %s", wellKnownKey, ledger)
	}
}
