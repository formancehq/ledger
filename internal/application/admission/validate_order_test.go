package admission

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestValidateOrder_LedgerName(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		order   *raftcmdpb.Order
		wantErr error
	}{
		{
			name: "valid CreateLedger",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
							CreateLedger: &raftcmdpb.CreateLedgerOrder{},
						},
					},
				},
			},
		},
		{
			name: "null byte in CreateLedger name",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "ledger\x00evil",
						Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
							CreateLedger: &raftcmdpb.CreateLedgerOrder{},
						},
					},
				},
			},
			wantErr: domain.ErrLedgerNameInvalidChar,
		},
		{
			name: "empty CreateLedger name",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "",
						Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
							CreateLedger: &raftcmdpb.CreateLedgerOrder{},
						},
					},
				},
			},
			wantErr: domain.ErrLedgerNameRequired,
		},
		{
			name: "null byte in Apply ledger",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "bad\x00name",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{},
						},
					},
				},
			},
			wantErr: domain.ErrLedgerNameInvalidChar,
		},
		{
			name: "null byte in SaveNumscript ledger",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "ns\x00bad",
						Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{
							SaveNumscript: &raftcmdpb.SaveNumscriptOrder{},
						},
					},
				},
			},
			wantErr: domain.ErrLedgerNameInvalidChar,
		},
		{
			name: "valid order without ledger (CloseChapter)",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_SystemScoped{
					SystemScoped: &raftcmdpb.SystemScopedOrder{
						Payload: &raftcmdpb.SystemScopedOrder_CloseChapter{
							CloseChapter: &raftcmdpb.CloseChapterOrder{},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateOrder(tt.order)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateOrder_PreparedQueryPayload pins the regression flagged on
// PR #522: after moving `ledger` off `common.PreparedQuery` onto the
// wrapper, a Create with a valid wrapper ledger but a missing/empty
// `query` would otherwise reach the FSM and persist a nameless prepared
// query. The validator gate (mirrored on the FSM side) must reject these
// malformed payloads at admission.
func TestValidateOrder_PreparedQueryPayload(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		order   *raftcmdpb.Order
		wantErr error
	}{
		{
			name: "create rejects nil query",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "l",
					Payload: &raftcmdpb.LedgerScopedOrder_CreatePreparedQuery{
						CreatePreparedQuery: &raftcmdpb.CreatePreparedQueryOrder{},
					},
				}},
			},
			wantErr: domain.ErrPreparedQueryRequired,
		},
		{
			name: "create rejects empty name",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "l",
					Payload: &raftcmdpb.LedgerScopedOrder_CreatePreparedQuery{
						CreatePreparedQuery: &raftcmdpb.CreatePreparedQueryOrder{
							Query: &commonpb.PreparedQuery{}, // empty name
						},
					},
				}},
			},
			wantErr: domain.ErrPreparedQueryNameRequired,
		},
		{
			// EN-1504 blocker: QUERY_TARGET_AUDIT is a valid enum value a gRPC
			// caller can set, but the prepared-query executor cannot run it.
			// Admission must reject it before it is persisted.
			name: "create rejects non-executable AUDIT target",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "l",
					Payload: &raftcmdpb.LedgerScopedOrder_CreatePreparedQuery{
						CreatePreparedQuery: &raftcmdpb.CreatePreparedQueryOrder{
							Query: &commonpb.PreparedQuery{
								Name:   "q",
								Target: commonpb.QueryTarget_QUERY_TARGET_AUDIT,
							},
						},
					},
				}},
			},
			wantErr: domain.ErrPreparedQueryTargetUnsupported,
		},
		{
			name: "update rejects empty name",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "l",
					Payload: &raftcmdpb.LedgerScopedOrder_UpdatePreparedQuery{
						UpdatePreparedQuery: &raftcmdpb.UpdatePreparedQueryOrder{},
					},
				}},
			},
			wantErr: domain.ErrPreparedQueryNameRequired,
		},
		{
			name: "delete rejects empty name",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "l",
					Payload: &raftcmdpb.LedgerScopedOrder_DeletePreparedQuery{
						DeletePreparedQuery: &raftcmdpb.DeletePreparedQueryOrder{},
					},
				}},
			},
			wantErr: domain.ErrPreparedQueryNameRequired,
		},
		{
			name: "create rejects name with control character",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "l",
					Payload: &raftcmdpb.LedgerScopedOrder_CreatePreparedQuery{
						CreatePreparedQuery: &raftcmdpb.CreatePreparedQueryOrder{
							Query: &commonpb.PreparedQuery{Name: "bad\nname"},
						},
					},
				}},
			},
			wantErr: domain.ErrPreparedQueryNameInvalidChar,
		},
		{
			name: "create accepts a well-formed name",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
					Ledger: "l",
					Payload: &raftcmdpb.LedgerScopedOrder_CreatePreparedQuery{
						CreatePreparedQuery: &raftcmdpb.CreatePreparedQueryOrder{
							Query: &commonpb.PreparedQuery{Name: "ok", Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS},
						},
					},
				}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateOrder(tt.order)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateOrder_MetadataKeys(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		order   *raftcmdpb.Order
		wantErr error
	}{
		{
			name: "valid metadata in CreateTransaction",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{
									// Content source required so the structural gate
									// (validateOrderContent) doesn't reject the order
									// before metadata is even looked at.
									Postings: []*commonpb.Posting{{
										Source:      "world",
										Destination: "users:alice",
										Amount:      commonpb.NewUint256FromUint64(1),
										Asset:       "USD",
									}},
									Metadata: map[string]*commonpb.MetadataValue{
										"category": {Type: &commonpb.MetadataValue_StringValue{StringValue: "test"}},
									},
								},
							},
							},
						},
					},
				},
			},
		},
		{
			name: "null byte in CreateTransaction metadata key",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{
									Metadata: map[string]*commonpb.MetadataValue{
										"bad\x00key": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}},
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyInvalidChar,
		},
		{
			name: "null byte in SaveLedgerMetadata key",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{
							SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{Metadata: map[string]*commonpb.MetadataValue{
								"bad\x00key": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyInvalidChar,
		},
		{
			name: "empty metadata key in DeleteMetadata",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
								DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{Key: ""},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyEmpty,
		},
		{
			name: "null byte in account metadata key",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{
									AccountMetadata: map[string]*commonpb.MetadataMap{
										"users:001": {
											Values: map[string]*commonpb.MetadataValue{
												"bad\x00key": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}},
											},
										},
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyInvalidChar,
		},
		// #322: revert orders silently bypassed validateApplyMetadataKeys
		// (no case for LedgerApplyOrder_RevertTransaction). A client could
		// thus ship empty / NUL-bearing keys directly into the canonical
		// Pebble layout via the revert log.
		{
			name: "null byte in RevertTransaction metadata key",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
								RevertTransaction: &raftcmdpb.RevertTransactionOrder{
									TransactionId: 1,
									Metadata: map[string]*commonpb.MetadataValue{
										"bad\x00key": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}},
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyInvalidChar,
		},
		{
			name: "empty metadata key in RevertTransaction",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
								RevertTransaction: &raftcmdpb.RevertTransactionOrder{
									TransactionId: 1,
									Metadata: map[string]*commonpb.MetadataValue{
										"": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}},
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataKeyEmpty,
		},
		{
			name: "valid metadata in RevertTransaction",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
								RevertTransaction: &raftcmdpb.RevertTransactionOrder{
									TransactionId: 1,
									Metadata: map[string]*commonpb.MetadataValue{
										"reason": {Type: &commonpb.MetadataValue_StringValue{StringValue: "wrong amount"}},
									},
								},
							},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateOrder(tt.order)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateOrder_MetadataValues(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		order   *raftcmdpb.Order
		wantErr error
	}{
		{
			name: "null byte in CreateTransaction metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{
									Metadata: map[string]*commonpb.MetadataValue{
										"category": commonpb.NewStringValue("safe\x00poison"),
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in CreateTransaction account metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: &raftcmdpb.CreateTransactionOrder{
									AccountMetadata: map[string]*commonpb.MetadataMap{
										"users:001": {
											Values: map[string]*commonpb.MetadataValue{
												"role": commonpb.NewStringValue("admin\x00poison"),
											},
										},
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in AddMetadata null original",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
								AddMetadata: &raftcmdpb.SaveMetadataOrder{
									Metadata: map[string]*commonpb.MetadataValue{
										"score": commonpb.NewNullValue("not\x00numeric"),
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in RevertTransaction metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
								RevertTransaction: &raftcmdpb.RevertTransactionOrder{
									TransactionId: 1,
									Metadata: map[string]*commonpb.MetadataValue{
										"reason": commonpb.NewStringValue("bad\x00reason"),
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in SaveLedgerMetadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{
							SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{Metadata: map[string]*commonpb.MetadataValue{
								"region": commonpb.NewStringValue("eu\x00west"),
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in MirrorIngest created transaction metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
							MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
								Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
									CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
										Metadata: map[string]*commonpb.MetadataValue{
											"category": commonpb.NewStringValue("safe\x00poison"),
										},
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in MirrorIngest account metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
							MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
								Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
									CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
										AccountMetadata: map[string]*commonpb.MetadataMap{
											"users:001": {
												Values: map[string]*commonpb.MetadataValue{
													"role": commonpb.NewStringValue("admin\x00poison"),
												},
											},
										},
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in MirrorIngest saved metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
							MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
								Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{
									SavedMetadata: &raftcmdpb.MirrorSavedMetadata{
										Metadata: map[string]*commonpb.MetadataValue{
											"status": commonpb.NewStringValue("active\x00poison"),
										},
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "null byte in MirrorIngest reverted transaction metadata value",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
							MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{
								Data: &raftcmdpb.MirrorLogEntry_RevertedTransaction{
									RevertedTransaction: &raftcmdpb.MirrorRevertedTransaction{
										Metadata: map[string]*commonpb.MetadataValue{
											"reason": commonpb.NewStringValue("bad\x00reason"),
										},
									},
								},
							},
							},
						},
					},
				},
			},
			wantErr: domain.ErrMetadataValueContainsNullByte,
		},
		{
			name: "valid typed metadata values",
			order: &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
								AddMetadata: &raftcmdpb.SaveMetadataOrder{
									Metadata: map[string]*commonpb.MetadataValue{
										"name":   commonpb.NewStringValue("alice"),
										"age":    commonpb.NewIntValue(42),
										"active": commonpb.NewBoolValue(true),
									},
								},
							},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := validateOrder(tt.order)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateOrderContent exercises the structural well-formedness gate
// added in #452: a CreateTransaction must declare at least one content
// source, and explicit postings cannot be combined with a script.
func TestValidateOrderContent(t *testing.T) {
	t.Parallel()

	posting := &commonpb.Posting{
		Source:      "world",
		Destination: "users:alice",
		Amount:      commonpb.NewUint256FromUint64(1),
		Asset:       "USD",
	}

	tests := []struct {
		name    string
		ct      *raftcmdpb.CreateTransactionOrder
		wantErr error
	}{
		{
			name:    "fully empty payload",
			ct:      &raftcmdpb.CreateTransactionOrder{},
			wantErr: domain.ErrEmptyTransaction,
		},
		{
			name: "empty inline script",
			ct: &raftcmdpb.CreateTransactionOrder{
				Script: &commonpb.Script{Plain: ""},
			},
			wantErr: domain.ErrEmptyTransaction,
		},
		{
			name: "metadata only, no content source",
			ct: &raftcmdpb.CreateTransactionOrder{
				Metadata: map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("v")},
			},
			wantErr: domain.ErrEmptyTransaction,
		},
		{
			name: "postings + inline script",
			ct: &raftcmdpb.CreateTransactionOrder{
				Postings: []*commonpb.Posting{posting},
				Script:   &commonpb.Script{Plain: "send [USD 1] (source = @world destination = @a)"},
			},
			wantErr: domain.ErrPostingsAndScriptConflict,
		},
		{
			name: "postings + scriptReference",
			ct: &raftcmdpb.CreateTransactionOrder{
				Postings:           []*commonpb.Posting{posting},
				NumscriptReference: &raftcmdpb.NumscriptReference{Name: "payment", Version: "1.0.0"},
			},
			wantErr: domain.ErrPostingsAndScriptConflict,
		},
		{
			name: "postings only",
			ct: &raftcmdpb.CreateTransactionOrder{
				Postings: []*commonpb.Posting{posting},
			},
		},
		{
			name: "inline script only",
			ct: &raftcmdpb.CreateTransactionOrder{
				Script: &commonpb.Script{Plain: "send [USD 1] (source = @world destination = @a)"},
			},
		},
		{
			name: "scriptReference only",
			ct: &raftcmdpb.CreateTransactionOrder{
				NumscriptReference: &raftcmdpb.NumscriptReference{Name: "payment", Version: "1.0.0"},
			},
		},
		{
			name: "empty scriptReference (no name)",
			ct: &raftcmdpb.CreateTransactionOrder{
				NumscriptReference: &raftcmdpb.NumscriptReference{},
			},
			wantErr: domain.ErrEmptyTransaction,
		},
		{
			name: "scriptReference with vars but no name",
			ct: &raftcmdpb.CreateTransactionOrder{
				NumscriptReference: &raftcmdpb.NumscriptReference{Vars: map[string]string{"k": "v"}},
			},
			wantErr: domain.ErrEmptyTransaction,
		},
		{
			name: "postings + empty scriptReference (nameless)",
			ct: &raftcmdpb.CreateTransactionOrder{
				Postings:           []*commonpb.Posting{posting},
				NumscriptReference: &raftcmdpb.NumscriptReference{},
			},
			wantErr: domain.ErrPostingsAndScriptConflict,
		},
		{
			name: "postings + scriptReference with vars but no name",
			ct: &raftcmdpb.CreateTransactionOrder{
				Postings:           []*commonpb.Posting{posting},
				NumscriptReference: &raftcmdpb.NumscriptReference{Vars: map[string]string{"k": "v"}},
			},
			wantErr: domain.ErrPostingsAndScriptConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			order := &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_Apply{
							Apply: &raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
								CreateTransaction: tt.ct,
							},
							},
						},
					},
				},
			}

			err := validateOrder(order)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

func TestValidateOrder_MirrorIAMRegion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		src     *commonpb.MirrorSourceConfig
		wantErr error
	}{
		{
			name: "no mirror source",
			src:  nil,
		},
		{
			name: "postgres mirror without IAM auth",
			src: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{
				Postgres: &commonpb.PostgresMirrorSourceConfig{Dsn: "postgres://user:pass@host:5432/db"},
			}},
		},
		{
			name: "postgres mirror with IAM auth and region",
			src: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{
				Postgres: &commonpb.PostgresMirrorSourceConfig{
					Dsn:        "postgres://iam-user@host:5432/db?sslmode=require",
					AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: "eu-west-1"},
				},
			}},
		},
		{
			name: "postgres mirror with IAM auth missing region rejected at admission",
			src: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{
				Postgres: &commonpb.PostgresMirrorSourceConfig{
					Dsn:        "postgres://iam-user@host:5432/db?sslmode=require",
					AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: ""},
				},
			}},
			wantErr: ErrMirrorIAMRegionRequired,
		},
		{
			name: "postgres mirror with IAM auth on non-TLS sslmode rejected at admission",
			src: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{
				Postgres: &commonpb.PostgresMirrorSourceConfig{
					Dsn:        "postgres://iam-user@host:5432/db?sslmode=disable",
					AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: "eu-west-1"},
				},
			}},
			wantErr: ErrMirrorIAMRequiresTLS,
		},
		{
			name: "postgres mirror with IAM auth on unset sslmode rejected at admission",
			src: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{
				Postgres: &commonpb.PostgresMirrorSourceConfig{
					Dsn:        "postgres://iam-user@host:5432/db",
					AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: "eu-west-1"},
				},
			}},
			wantErr: ErrMirrorIAMRequiresTLS,
		},
		{
			name: "postgres mirror with IAM auth on libpq keyword=value DSN rejected at admission",
			src: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{
				Postgres: &commonpb.PostgresMirrorSourceConfig{
					Dsn:        `host=db.example.com user=iam-user dbname=ledger sslmode=require`,
					AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: "eu-west-1"},
				},
			}},
			wantErr: ErrMirrorIAMRequiresTLS,
		},
		{
			name: "http mirror source unaffected",
			src: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Http{
				Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "http://v2:3068"},
			}},
		},
		{
			name: "valid rewrite rules accepted",
			src: &commonpb.MirrorSourceConfig{
				Type: &commonpb.MirrorSourceConfig_Http{
					Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "http://v2:3068"},
				},
				RewriteRules: []*commonpb.MirrorRewriteRule{
					anyRuleRewriteAddress(":worker:\\d+", ""),
					createdRuleSetMetadata(`log.metadata["type"].string_value == "payout"`, "category", "external", true),
				},
			},
		},
		{
			name: "unset scope rejected at admission",
			src: &commonpb.MirrorSourceConfig{
				Type: &commonpb.MirrorSourceConfig_Http{
					Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "http://v2:3068"},
				},
				RewriteRules: []*commonpb.MirrorRewriteRule{
					{Stop: true},
				},
			},
			wantErr: ErrMirrorRewriteRuleInvalid,
		},
		{
			name: "invalid cel expression rejected at admission",
			src: &commonpb.MirrorSourceConfig{
				Type: &commonpb.MirrorSourceConfig_Http{
					Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "http://v2:3068"},
				},
				RewriteRules: []*commonpb.MirrorRewriteRule{
					anyRuleWithMatch(`this is not valid cel`),
				},
			},
			wantErr: ErrMirrorRewriteRuleInvalid,
		},
		{
			name: "non-boolean match rejected at admission",
			src: &commonpb.MirrorSourceConfig{
				Type: &commonpb.MirrorSourceConfig_Http{
					Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "http://v2:3068"},
				},
				RewriteRules: []*commonpb.MirrorRewriteRule{
					anyRuleWithMatch(`"a string"`),
				},
			},
			wantErr: ErrMirrorRewriteRuleInvalid,
		},
		{
			name: "invalid literal regex pattern rejected at admission",
			src: &commonpb.MirrorSourceConfig{
				Type: &commonpb.MirrorSourceConfig_Http{
					Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "http://v2:3068"},
				},
				RewriteRules: []*commonpb.MirrorRewriteRule{
					anyRuleRewriteAddress("(", ""),
				},
			},
			wantErr: ErrMirrorRewriteRuleInvalid,
		},
		{
			name: "invalid literal metadata key rejected at admission",
			src: &commonpb.MirrorSourceConfig{
				Type: &commonpb.MirrorSourceConfig_Http{
					Http: &commonpb.HttpMirrorSourceConfig{BaseUrl: "http://v2:3068"},
				},
				RewriteRules: []*commonpb.MirrorRewriteRule{
					createdRuleSetMetadata("", "bad key", "v", false),
				},
			},
			wantErr: ErrMirrorRewriteRuleInvalid,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			order := &raftcmdpb.Order{
				Type: &raftcmdpb.Order_LedgerScoped{
					LedgerScoped: &raftcmdpb.LedgerScopedOrder{
						Ledger: "default",
						Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
							CreateLedger: &raftcmdpb.CreateLedgerOrder{
								Mode:         commonpb.LedgerMode_LEDGER_MODE_MIRROR,
								MirrorSource: tt.src,
							},
						},
					},
				},
			}

			err := validateOrder(order)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// TestValidateOrder_MirrorIAMRejectsPGSSLMODEBypass reproduces the exact
// bypass NumaryBot reported: pgxpool.ParseConfig folds PGSSLMODE from the
// process env, so a DSN without sslmode= would inherit that env var and
// pass a naive TLSConfig check. The admission gate must anchor the TLS
// requirement in the raw DSN, independent of pod env.
//
// This test cannot share a t.Parallel outer with the table above because
// t.Setenv forbids parallel siblings mutating the process env.
func TestValidateOrder_MirrorIAMRejectsPGSSLMODEBypass(t *testing.T) {
	t.Setenv("PGSSLMODE", "require")

	order := &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: "default",
				Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{
					CreateLedger: &raftcmdpb.CreateLedgerOrder{
						Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
						MirrorSource: &commonpb.MirrorSourceConfig{Type: &commonpb.MirrorSourceConfig_Postgres{
							Postgres: &commonpb.PostgresMirrorSourceConfig{
								Dsn:        "postgres://iam-user@host:5432/db",
								AwsIamAuth: &commonpb.PostgresAwsIamAuth{Region: "eu-west-1"},
							},
						}},
					},
				},
			},
		},
	}

	err := validateOrder(order)
	require.ErrorIs(t, err, ErrMirrorIAMRequiresTLS,
		"PGSSLMODE=require in the pod env must not satisfy the admission TLS gate — the persisted DSN must carry sslmode= itself")
}
