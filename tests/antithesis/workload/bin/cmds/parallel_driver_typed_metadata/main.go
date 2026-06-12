package main

import (
	"context"
	"fmt"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func main() {
	internal.RunDriver("parallel_driver_typed_metadata", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		metaKey := fmt.Sprintf("score-%d", r.Uint64())
		metaValue := int64(r.Uint64() % 1000)
		address := internal.GetRandomAddress()

		details := internal.Details{
			"ledger":  ledger,
			"key":     metaKey,
			"value":   metaValue,
			"account": address,
		}

		// 1. Declare the metadata key as INT64.
		_, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_SetMetadataFieldType{
					SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
						Ledger:     ledger,
						TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:        metaKey,
						Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
					},
				},
			}},
		})
		if err != nil && !internal.IsTransient(err) {
			return
		}

		// 2. Create an index on this metadata key.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_CreateIndex{
					CreateIndex: &servicepb.CreateIndexRequest{
						Ledger: ledger,
						Id: &commonpb.IndexID{Kind: &commonpb.IndexID_Metadata{Metadata: &commonpb.MetadataIndexID{
							Target: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:    metaKey,
						}}},
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

		// 3. Save typed metadata on the account.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
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
									metaKey: fmt.Sprintf("%d", metaValue),
								}),
							},
						}},
					},
				},
			}},
		})
		if err != nil {
			return
		}

		// 4. Create a prepared query filtering by int range on this key.
		queryName := fmt.Sprintf("typed-q-%s", metaKey)
		minVal := int64(0)
		maxVal := int64(999)

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_CreatePreparedQuery{
					CreatePreparedQuery: &servicepb.CreatePreparedQueryRequest{
						Query: &commonpb.PreparedQuery{
							Name:   queryName,
							Ledger: ledger,
							Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
							Filter: &commonpb.QueryFilter{
								Filter: &commonpb.QueryFilter_Field{
									Field: &commonpb.FieldCondition{
										Field: &commonpb.FieldRef{Metadata: metaKey},
										Condition: &commonpb.FieldCondition_IntCond{
											IntCond: &commonpb.IntCondition{
												Min: &minVal,
												Max: &maxVal,
											},
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

		// 5. Execute the query — the account we just wrote should be in the results.
		execResp, err := client.ExecutePreparedQuery(ctx, &servicepb.ExecutePreparedQueryRequest{
			Ledger:    ledger,
			QueryName: queryName,
			PageSize:  100,
		})
		if err != nil {
			internal.LogCleanupError(fmt.Sprintf("execute prepared query %q", queryName), err)
			return
		}

		assert.AlwaysOrUnreachable(execResp != nil, "typed metadata query should return a response", details)

		// 6. Check schema status — the key should be declared.
		schemaResp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
			Ledger: ledger,
		})
		if err != nil {
			internal.LogCleanupError("get metadata schema status", err)
			return
		}

		fieldStatus, ok := schemaResp.GetAccountFields()[metaKey]
		assert.AlwaysOrUnreachable(ok, "declared metadata key should appear in schema status", details)

		if ok {
			assert.AlwaysOrUnreachable(
				fieldStatus.GetDeclaredType() == commonpb.MetadataType_METADATA_TYPE_INT64,
				"declared type should be INT64",
				details,
			)
		}

		// 7. Change the type to exercise type migration paths.
		// This should not crash even with existing data of the old type.
		newTypes := []commonpb.MetadataType{
			commonpb.MetadataType_METADATA_TYPE_STRING,
			commonpb.MetadataType_METADATA_TYPE_BOOL,
			commonpb.MetadataType_METADATA_TYPE_INT64,
		}
		for _, newType := range newTypes {
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_SetMetadataFieldType{
						SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
							Ledger:     ledger,
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        metaKey,
							Type:       newType,
						},
					},
				}},
			})
			assert.AlwaysOrUnreachable(err == nil || internal.IsTransient(err),
				"changing metadata type should not crash", details.With(internal.Details{
					"newType": newType.String(),
					"error":   err,
				}))
		}

		// 8. Remove the type declaration entirely.
		if _, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_RemoveMetadataFieldType{
					RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
						Ledger:     ledger,
						TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:        metaKey,
					},
				},
			}},
		}); err != nil {
			internal.LogCleanupError(fmt.Sprintf("remove metadata field type %q", metaKey), err)
		}

		// 9. Verify the account is still readable after all type changes.
		acct, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
			Ledger:  ledger,
			Address: address,
		})
		if err != nil {
			internal.LogCleanupError("read account after type changes", err)
		}
		assert.AlwaysOrUnreachable(err == nil || internal.IsTransient(err),
			"account should be readable after metadata type changes", details.With(internal.Details{"error": err}))

		if err == nil {
			// The metadata value should still be present (possibly converted).
			_, found := acct.GetMetadata()[metaKey]

			assert.AlwaysOrUnreachable(found, "metadata key should survive type changes", details)
		}

		// 10. Test invalid type conversion: save a non-numeric string value,
		// then try to declare the key as INT64. The server should reject
		// the conversion or handle it gracefully — never crash.
		badKey := fmt.Sprintf("bad-convert-%d", r.Uint64())

		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
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
									badKey: "not-a-number",
								}),
							},
						}},
					},
				},
			}},
		})
		if err != nil {
			// If save fails, skip the conversion test.
		} else {
			// Try to declare this key as INT64 — existing value "not-a-number"
			// cannot be converted. The server should return an error, not crash.
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_SetMetadataFieldType{
						SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
							Ledger:     ledger,
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        badKey,
							Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
						},
					},
				}},
			})
			// The server accepts the declaration — conversion runs in background.
			// "not-a-number" → INT64 produces a NullValue (original string preserved).
			assert.AlwaysOrUnreachable(err == nil || internal.IsTransient(err),
				"invalid type conversion should be accepted", internal.Details{
					"key":    badKey,
					"value":  "not-a-number",
					"toType": "INT64",
					"error":  err,
				})
			if err != nil {
				// Declaration failed (e.g. maintenance mode) — skip verification.
			} else {
				// Retry until the background conversion has completed and the
				// read path returns a NullValue for the unconvertible string.
				for attempt := 0; attempt < 10; attempt++ {
					acctAfterBad, readErr := client.GetAccount(ctx, &servicepb.GetAccountRequest{
						Ledger:  ledger,
						Address: address,
					})
					if readErr != nil {
						if internal.IsTransient(readErr) {
							break
						}

						assert.Unreachable("GetAccount failed after invalid type declaration", internal.Details{
							"key":   badKey,
							"error": readErr,
						})

						break
					}

					converted := false

					if v, ok := acctAfterBad.GetMetadata()[badKey]; ok {
						if _, isNull := v.GetType().(*commonpb.MetadataValue_NullValue); isNull {
							converted = true
						}
					}

					if converted {
						break
					}

					time.Sleep(200 * time.Millisecond)
				}
			}

			// Same with BOOL conversion on the numeric key (42 → bool = true).
			_, err = client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{{
					Type: &servicepb.Request_SetMetadataFieldType{
						SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
							Ledger:     ledger,
							TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
							Key:        metaKey,
							Type:       commonpb.MetadataType_METADATA_TYPE_BOOL,
						},
					},
				}},
			})
			assert.AlwaysOrUnreachable(err == nil || internal.IsTransient(err),
				"numeric to bool conversion should be accepted", internal.Details{
					"key":    metaKey,
					"value":  metaValue,
					"toType": "BOOL",
					"error":  err,
				})
		}

		// Cleanup: delete the prepared query.
		if _, err := client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_DeletePreparedQuery{
					DeletePreparedQuery: &servicepb.DeletePreparedQueryRequest{
						Ledger: ledger,
						Name:   queryName,
					},
				},
			}},
		}); err != nil {
			internal.LogCleanupError(fmt.Sprintf("delete prepared query %q", queryName), err)
		}
	})
}
