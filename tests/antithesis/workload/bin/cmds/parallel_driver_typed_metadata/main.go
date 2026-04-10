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
	internal.RunDriver("parallel_driver_typed_metadata", func(ctx context.Context, client servicepb.BucketServiceClient, ledger string) {
		r := internal.Rand()
		metaKey := fmt.Sprintf("score-%d", r.Uint64()%20)
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
		if err != nil && !internal.IsUnavailable(err) {
			return
		}

		// 2. Create an index on this metadata key.
		_, err = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_CreateIndex{
					CreateIndex: &servicepb.CreateIndexRequest{
						Ledger: ledger,
						Index: &servicepb.CreateIndexRequest_Account{
							Account: &commonpb.AccountIndex{
								Kind: &commonpb.AccountIndex_MetadataKey{
									MetadataKey: metaKey,
								},
							},
						},
					},
				},
			}},
		})
		if err != nil && !internal.IsUnavailable(err) {
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
						Data: &servicepb.LedgerApplyRequest_AddMetadata{
							AddMetadata: &commonpb.SaveMetadataCommand{
								Target: &commonpb.Target{
									Target: &commonpb.Target_Account{
										Account: &commonpb.TargetAccount{Addr: address},
									},
								},
								Metadata: commonpb.MetadataSetFromMap(map[string]string{
									metaKey: fmt.Sprintf("%d", metaValue),
								}),
							},
						},
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
		if err != nil && !internal.IsUnavailable(err) {
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
			return
		}

		assert.AlwaysOrUnreachable(execResp != nil, "typed metadata query should return a response", details)

		// 6. Check schema status — the key should be declared.
		schemaResp, err := client.GetMetadataSchemaStatus(ctx, &servicepb.GetMetadataSchemaStatusRequest{
			Ledger: ledger,
		})
		if err != nil {
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
			assert.AlwaysOrUnreachable(err == nil || internal.IsUnavailable(err),
				"changing metadata type should not crash", details.With(internal.Details{
					"newType": newType.String(),
					"error":   err,
				}))
		}

		// 8. Remove the type declaration entirely.
		_, _ = client.Apply(ctx, &servicepb.ApplyRequest{
			Requests: []*servicepb.Request{{
				Type: &servicepb.Request_RemoveMetadataFieldType{
					RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
						Ledger:     ledger,
						TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
						Key:        metaKey,
					},
				},
			}},
		})

		// 9. Verify the account is still readable after all type changes.
		acct, err := client.GetAccount(ctx, &servicepb.GetAccountRequest{
			Ledger:  ledger,
			Address: address,
		})
		assert.AlwaysOrUnreachable(err == nil || internal.IsUnavailable(err),
			"account should be readable after metadata type changes", details.With(internal.Details{"error": err}))

		if err == nil {
			// The metadata value should still be present (possibly converted).
			found := false
			for _, m := range acct.GetMetadata().GetMetadata() {
				if m.GetKey() == metaKey {
					found = true

					break
				}
			}

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
						Data: &servicepb.LedgerApplyRequest_AddMetadata{
							AddMetadata: &commonpb.SaveMetadataCommand{
								Target: &commonpb.Target{
									Target: &commonpb.Target_Account{
										Account: &commonpb.TargetAccount{Addr: address},
									},
								},
								Metadata: commonpb.MetadataSetFromMap(map[string]string{
									badKey: "not-a-number",
								}),
							},
						},
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
			assert.AlwaysOrUnreachable(err == nil || internal.IsUnavailable(err),
				"invalid type conversion should be accepted", internal.Details{
					"key":    badKey,
					"value":  "not-a-number",
					"toType": "INT64",
					"error":  err,
				})

			// Read the account and verify the value is readable.
			// The read path enforces the schema lazily: if background conversion
			// hasn't run yet, enforceAccountSchema converts on the fly.
			// For "not-a-number" → INT64, the result should be a NullValue.
			acctAfterBad, readErr := client.GetAccount(ctx, &servicepb.GetAccountRequest{
				Ledger:  ledger,
				Address: address,
			})
			assert.AlwaysOrUnreachable(readErr == nil || internal.IsUnavailable(readErr),
				"account should be readable after invalid type declaration", internal.Details{
					"key":   badKey,
					"error": readErr,
				})

			if readErr == nil {
				for _, m := range acctAfterBad.GetMetadata().GetMetadata() {
					if m.GetKey() == badKey {
						// After declaring "not-a-number" as INT64, the read path
						// should convert it to a NullValue (preserving the original).
						_, isNull := m.GetValue().GetType().(*commonpb.MetadataValue_NullValue)
						assert.AlwaysOrUnreachable(isNull, "unconvertible string should become NullValue on read", internal.Details{
							"key":       badKey,
							"valueType": fmt.Sprintf("%T", m.GetValue().GetType()),
						})

						break
					}
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
			assert.AlwaysOrUnreachable(err == nil || internal.IsUnavailable(err),
				"numeric to bool conversion should be accepted", internal.Details{
					"key":    metaKey,
					"value":  metaValue,
					"toType": "BOOL",
					"error":  err,
				})
		}

		// Cleanup: delete the prepared query.
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
