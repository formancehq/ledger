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
			Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
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
			}),
		})
		if err != nil {
			if internal.IsTransient(err) {
				return
			}

			log.Printf("metadata-conversion: write failed: %s", err)

			return
		}

		// 1a. Precondition for the step-3 assertion: confirm the account
		// is observably present with our key on a connection-served node
		// BEFORE running the type change. Without this, a sibling driver
		// that wipes/overwrites this account's metadata mid-flight (the
		// addresses come from a shared "users:N" pool, see
		// internal.GetRandomAddress) can make step 3 read a state that
		// was never the post-condition of step 2 — turning an honest
		// concurrency race into a spurious "account must be readable"
		// failure.
		//
		// Skip cases:
		//  - transient read error: fault window, retry on a future run
		//  - missing key in the response: sibling-race / follower-lag,
		//    precondition unmet without a SUT bug
		//
		// A non-transient read error here is NOT a precondition issue:
		// it is the same class of bug the step-3 assertion is designed
		// to catch (GetAccount must not fail non-transiently on an
		// account that just received a successful write). Assert it
		// instead of swallowing — see NumaryBot review on PR #454.
		preAcct, preErr := client.GetAccount(ctx, &servicepb.GetAccountRequest{
			Ledger:  ledger,
			Address: address,
		})
		if preErr != nil {
			if internal.IsTransient(preErr) {
				return
			}
			assert.Unreachable("account must be readable after a metadata write",
				details.With(internal.Details{"error": preErr, "phase": "precondition"}))

			return
		}
		if _, hasKey := preAcct.GetMetadata()[wellKnownKey]; !hasKey {
			// Concurrent driver removed/overwrote the key, or the read
			// landed on a node whose Pebble has not yet caught up to our
			// write. The post-condition we want to assert in step 3 is
			// no longer well-defined for this run.
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
			// The schema entry must report one of the two valid statuses.
			status := fieldStatus.GetStatus()
			assert.AlwaysOrUnreachable(
				status == commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING ||
					status == commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE,
				"metadata field status must be CONVERTING or COMPLETE",
				details.With(internal.Details{
					"declaredType": fieldStatus.GetDeclaredType().String(),
					"status":       status.String(),
				}),
			)
		}

		assert.Reachable("metadata conversion check passed", details)
	})
}

func declareType(ctx context.Context, client servicepb.BucketServiceClient, ledger string, metaType commonpb.MetadataType, details internal.Details) {
	details = details.With(internal.Details{"declareType": metaType.String()})

	_, err := client.Apply(ctx, &servicepb.ApplyRequest{
		Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
			Type: &servicepb.Request_SetMetadataFieldType{
				SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
					Ledger:     ledger,
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        wellKnownKey,
					Type:       metaType,
				},
			},
		}),
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
		Envelopes: servicepb.UnsignedEnvelopes(&servicepb.Request{
			Type: &servicepb.Request_RemoveMetadataFieldType{
				RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
					Ledger:     ledger,
					TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
					Key:        wellKnownKey,
				},
			},
		}),
	})

	assert.AlwaysOrUnreachable(err == nil || internal.IsTransient(err),
		"removing metadata type should not crash",
		details.With(internal.Details{"error": err}),
	)

	if err == nil {
		log.Printf("metadata-conversion: removed type for %s on %s", wellKnownKey, ledger)
	}
}
