package admission

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// metadataTestLimits is small enough to make the boundaries readable, and
// consistent (key/value <= entity <= command) like any committed policy.
func metadataTestLimits() domain.MetadataLimits {
	return domain.MetadataLimits{
		MaxEntriesPerEntity:     2,
		MaxKeyBytes:             8,
		MaxValueBytes:           16,
		MaxTotalBytesPerEntity:  40,
		MaxTotalBytesPerCommand: 60,
	}
}

func metadataStringValue(s string) *commonpb.MetadataValue {
	return &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: s}}
}

// metadataApplyOrder wraps a ledger-apply payload in the ledger-scoped
// envelope. The generated oneof interfaces are unexported, so these helpers
// take the assembled inner message rather than the oneof wrapper.
func metadataApplyOrder(apply *raftcmdpb.LedgerApplyOrder) *raftcmdpb.Order {
	return metadataLedgerScopedOrder(&raftcmdpb.LedgerScopedOrder{
		Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: apply},
	})
}

func metadataLedgerScopedOrder(ls *raftcmdpb.LedgerScopedOrder) *raftcmdpb.Order {
	ls.Ledger = "default"

	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: ls}}
}

// metadataMirrorOrder wraps a mirror log entry, the ingest path's order shape.
func metadataMirrorOrder(entry *raftcmdpb.MirrorLogEntry) *raftcmdpb.Order {
	return metadataLedgerScopedOrder(&raftcmdpb.LedgerScopedOrder{
		Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
			MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: entry},
		},
	})
}

// metadataAddOrder is the AddMetadata order, whose payload message is
// SaveMetadataOrder.
func metadataAddOrder(m map[string]*commonpb.MetadataValue) *raftcmdpb.Order {
	return metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{
			AddMetadata: &raftcmdpb.SaveMetadataOrder{Metadata: m},
		},
	})
}

// Every order shape that can carry metadata is bounded, and each accepts its
// exact boundary. A shape reachable from a public entry path but missed by the
// walk would be an unbounded hole, so the acceptance cases pin the reach of the
// traversal as much as the rejection cases pin the ceilings.
func TestValidateCommandMetadata_EveryOrderShape(t *testing.T) {
	t.Parallel()

	atValueCeiling := metadataStringValue(strings.Repeat("v", 16))
	overValueCeiling := metadataStringValue(strings.Repeat("v", 17))
	overKeyCeiling := strings.Repeat("k", 9)

	tests := []struct {
		name          string
		order         *raftcmdpb.Order
		wantDimension string
	}{
		{
			name: "create transaction metadata accepted at the ceiling",
			order: metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
					CreateTransaction: &raftcmdpb.CreateTransactionOrder{
						Metadata: map[string]*commonpb.MetadataValue{"k": atValueCeiling},
					},
				},
			}),
		},
		{
			name: "create transaction metadata over the value ceiling",
			order: metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
					CreateTransaction: &raftcmdpb.CreateTransactionOrder{
						Metadata: map[string]*commonpb.MetadataValue{"k": overValueCeiling},
					},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionValue,
		},
		{
			name: "create transaction account metadata over the entry-count ceiling",
			order: metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
					CreateTransaction: &raftcmdpb.CreateTransactionOrder{
						AccountMetadata: map[string]*commonpb.MetadataMap{
							"users:alice": {Values: map[string]*commonpb.MetadataValue{
								"a": metadataStringValue("1"),
								"b": metadataStringValue("2"),
								"c": metadataStringValue("3"),
							}},
						},
					},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionEntries,
		},
		{
			name: "revert transaction metadata over the value ceiling",
			order: metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{
					RevertTransaction: &raftcmdpb.RevertTransactionOrder{
						Metadata: map[string]*commonpb.MetadataValue{"k": overValueCeiling},
					},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionValue,
		},
		{
			name:  "add metadata accepted at the ceiling",
			order: metadataAddOrder(map[string]*commonpb.MetadataValue{"k": atValueCeiling}),
		},
		{
			name:          "add metadata over the value ceiling",
			order:         metadataAddOrder(map[string]*commonpb.MetadataValue{"k": overValueCeiling}),
			wantDimension: domain.MetadataLimitDimensionValue,
		},
		{
			name: "delete metadata key over the key ceiling",
			order: metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
					DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{Key: overKeyCeiling},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionKey,
		},
		{
			name: "set metadata field type key over the key ceiling",
			order: metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{
					SetMetadataFieldType: &raftcmdpb.SetMetadataFieldTypeOrder{Key: overKeyCeiling},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionKey,
		},
		{
			name: "remove metadata field type key over the key ceiling",
			order: metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
				Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{
					RemoveMetadataFieldType: &raftcmdpb.RemoveMetadataFieldTypeOrder{Key: overKeyCeiling},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionKey,
		},
		{
			name: "save ledger metadata over the value ceiling",
			order: metadataLedgerScopedOrder(&raftcmdpb.LedgerScopedOrder{
				Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{
					SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{
						Metadata: map[string]*commonpb.MetadataValue{"k": overValueCeiling},
					},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionValue,
		},
		{
			name: "delete ledger metadata key over the key ceiling",
			order: metadataLedgerScopedOrder(&raftcmdpb.LedgerScopedOrder{
				Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata{
					DeleteLedgerMetadata: &raftcmdpb.DeleteLedgerMetadataOrder{Key: overKeyCeiling},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionKey,
		},
		{
			name: "mirror ingest created transaction over the value ceiling",
			order: metadataMirrorOrder(&raftcmdpb.MirrorLogEntry{
				Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
					CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
						Metadata: map[string]*commonpb.MetadataValue{"k": overValueCeiling},
					},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionValue,
		},
		{
			name: "mirror ingest account metadata over the value ceiling",
			order: metadataMirrorOrder(&raftcmdpb.MirrorLogEntry{
				Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
					CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{
						AccountMetadata: map[string]*commonpb.MetadataMap{
							"users:alice": {Values: map[string]*commonpb.MetadataValue{"k": overValueCeiling}},
						},
					},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionValue,
		},
		{
			name: "mirror ingest saved metadata over the value ceiling",
			order: metadataMirrorOrder(&raftcmdpb.MirrorLogEntry{
				Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{
					SavedMetadata: &raftcmdpb.MirrorSavedMetadata{
						Metadata: map[string]*commonpb.MetadataValue{"k": overValueCeiling},
					},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionValue,
		},
		{
			name: "mirror ingest reverted transaction over the value ceiling",
			order: metadataMirrorOrder(&raftcmdpb.MirrorLogEntry{
				Data: &raftcmdpb.MirrorLogEntry_RevertedTransaction{
					RevertedTransaction: &raftcmdpb.MirrorRevertedTransaction{
						Metadata: map[string]*commonpb.MetadataValue{"k": overValueCeiling},
					},
				},
			}),
			wantDimension: domain.MetadataLimitDimensionValue,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			err := validateCommandMetadata([]*raftcmdpb.Order{tc.order}, metadataTestLimits())

			if tc.wantDimension == "" {
				require.NoError(t, err, "the boundary itself must be accepted")

				return
			}

			require.Error(t, err)

			var d domain.Describable
			require.ErrorAs(t, err, &d)
			require.Equal(t, domain.ErrReasonMetadataLimitExceeded, d.Reason())
			require.Equal(t, domain.KindValidation, domain.Kind(d))
			require.Equal(t, tc.wantDimension, d.Metadata()["dimension"])
		})
	}
}

// An account-scoped failure names the account, so a caller sending metadata for
// several accounts learns which one was rejected.
func TestValidateCommandMetadata_NamesOffendingAccount(t *testing.T) {
	t.Parallel()

	order := metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
			CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				AccountMetadata: map[string]*commonpb.MetadataMap{
					"users:alice": {Values: map[string]*commonpb.MetadataValue{"k": metadataStringValue("small")}},
					"users:bob":   {Values: map[string]*commonpb.MetadataValue{"k": metadataStringValue(strings.Repeat("v", 17))}},
				},
			},
		},
	})

	err := validateCommandMetadata([]*raftcmdpb.Order{order}, metadataTestLimits())
	require.Error(t, err)

	var d domain.Describable
	require.ErrorAs(t, err, &d)
	require.Equal(t, "users:bob", d.Metadata()["account"])
}

// The per-command ceiling spans the whole batch: each order is individually
// legal, but together they exceed it. Without this, a caller could defeat the
// per-entity bound by spreading a payload over many entities in one command.
func TestValidateCommandMetadata_PerCommandTotalAcrossOrders(t *testing.T) {
	t.Parallel()

	limits := metadataTestLimits()

	// 8 + 16 = 24 bytes per order: one order fits the entity ceiling (40), two
	// fit the command ceiling (60) at 48 bytes, three do not at 72.
	orderWith := func(key string) *raftcmdpb.Order {
		return metadataAddOrder(map[string]*commonpb.MetadataValue{
			key: metadataStringValue(strings.Repeat("v", 16)),
		})
	}

	two := []*raftcmdpb.Order{orderWith("aaaaaaaa"), orderWith("bbbbbbbb")}
	require.NoError(t, validateCommandMetadata(two, limits))

	three := []*raftcmdpb.Order{two[0], two[1], orderWith("cccccccc")}

	err := validateCommandMetadata(three, limits)
	require.Error(t, err)

	var d domain.Describable
	require.ErrorAs(t, err, &d)
	require.Equal(t, domain.MetadataLimitDimensionCommand, d.Metadata()["dimension"])
	require.Equal(t, "60", d.Metadata()["limit"])
	require.Equal(t, "72", d.Metadata()["actual"])
}

// A bare metadata key counts toward the command total too: the delete and
// field-type orders carry a key and no value, and excluding them would let a
// batch of deletes carry unbounded bytes.
func TestOrderMetadataBytes(t *testing.T) {
	t.Parallel()

	require.Equal(t, uint64(1+5), domain.OrderMetadataSize(
		metadataAddOrder(map[string]*commonpb.MetadataValue{"k": metadataStringValue("value")})))

	require.Equal(t, uint64(3), domain.OrderMetadataSize(metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
			DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{Key: "abc"},
		},
	})))

	// An order carrying no metadata contributes nothing.
	require.Zero(t, domain.OrderMetadataSize(&raftcmdpb.Order{}))
}

// A committed policy without ceilings must reject rather than admit unbounded
// metadata — including for an empty command, so the failure cannot be dodged by
// sending nothing.
func TestValidateCommandMetadata_UnconfiguredLimitsReject(t *testing.T) {
	t.Parallel()

	order := metadataAddOrder(map[string]*commonpb.MetadataValue{"k": metadataStringValue("v")})

	require.ErrorIs(t,
		validateCommandMetadata([]*raftcmdpb.Order{order}, domain.MetadataLimits{}),
		domain.ErrMetadataLimitsUnconfigured,
	)

	require.ErrorIs(t,
		validateCommandMetadata(nil, domain.MetadataLimits{}),
		domain.ErrMetadataLimitsUnconfigured,
	)
}

// The size gate and the shape gate must agree on which maps exist, so both are
// driven by walkOrderMetadata. Measuring one order through the byte accounting
// and validating it through the shape gate pins that they see the same payload:
// a shape the walk misses would report zero bytes AND skip validation.
func TestMetadataWalkCoversTheSamePayloadForSizeAndShape(t *testing.T) {
	t.Parallel()

	order := metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
			CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				Metadata: map[string]*commonpb.MetadataValue{"tx": metadataStringValue("12345")},
				AccountMetadata: map[string]*commonpb.MetadataMap{
					"users:alice": {Values: map[string]*commonpb.MetadataValue{"acc": metadataStringValue("678")}},
				},
			},
		},
	})

	// (2 + 5) for the transaction map, (3 + 3) for the account map.
	require.Equal(t, uint64(7+6), domain.OrderMetadataSize(order))

	// The shape gate reaches both maps as well: a NUL byte in the account map is
	// rejected even though the transaction map is clean.
	dirty := metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
			CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				Metadata: map[string]*commonpb.MetadataValue{"tx": metadataStringValue("fine")},
				AccountMetadata: map[string]*commonpb.MetadataMap{
					"users:alice": {Values: map[string]*commonpb.MetadataValue{"bad\x00key": metadataStringValue("v")}},
				},
			},
		},
	})
	require.NotNil(t, validateOrderMetadata(dirty))
}

// A nil per-account map carries nothing; it is skipped rather than reported, and
// it must not make the walk fail or miscount.
func TestValidateCommandMetadata_NilAccountMapIsSkipped(t *testing.T) {
	t.Parallel()

	order := metadataApplyOrder(&raftcmdpb.LedgerApplyOrder{
		Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
			CreateTransaction: &raftcmdpb.CreateTransactionOrder{
				AccountMetadata: map[string]*commonpb.MetadataMap{"users:alice": nil},
			},
		},
	})

	require.NoError(t, validateCommandMetadata([]*raftcmdpb.Order{order}, metadataTestLimits()))
	require.Zero(t, domain.OrderMetadataSize(order))
}

func TestMirrorDeletedMetadataValidation(t *testing.T) {
	t.Parallel()

	orderWithKey := func(key string) *raftcmdpb.Order {
		return metadataMirrorOrder(&raftcmdpb.MirrorLogEntry{
			Data: &raftcmdpb.MirrorLogEntry_DeletedMetadata{
				DeletedMetadata: &raftcmdpb.MirrorDeletedMetadata{Key: key},
			},
		})
	}

	for _, key := range []string{"", "bad\x00key"} {
		t.Run("invalid key "+key, func(t *testing.T) {
			t.Parallel()
			err := validateOrder(orderWithKey(key))
			require.ErrorIs(t, err, domain.ValidateMetadataKey(key))
		})
	}

	limits := metadataTestLimits()
	atLimit := orderWithKey(strings.Repeat("k", int(limits.MaxKeyBytes)))
	require.NoError(t, validateOrder(atLimit))
	require.NoError(t, validateCommandMetadata([]*raftcmdpb.Order{atLimit}, limits))
	require.Equal(t, limits.MaxKeyBytes, domain.OrderMetadataSize(atLimit))

	err := validateCommandMetadata([]*raftcmdpb.Order{
		orderWithKey(strings.Repeat("k", int(limits.MaxKeyBytes)+1)),
	}, limits)
	var failure domain.Describable
	require.ErrorAs(t, err, &failure)
	require.Equal(t, domain.ErrReasonMetadataLimitExceeded, failure.Reason())
	require.Equal(t, domain.MetadataLimitDimensionKey, failure.Metadata()["dimension"])

	// Each deletion fits the key ceiling; the batch must still count all keys.
	orders := make([]*raftcmdpb.Order, 8)
	for i := range orders {
		orders[i] = atLimit
	}
	require.NoError(t, validateCommandMetadata(orders[:7], limits))
	err = validateCommandMetadata(orders, limits)
	require.ErrorAs(t, err, &failure)
	require.Equal(t, domain.ErrReasonMetadataLimitExceeded, failure.Reason())
	require.Equal(t, domain.MetadataLimitDimensionCommand, failure.Metadata()["dimension"])
	require.Equal(t, "64", failure.Metadata()["actual"])
}

func TestValidateOrderMetadataShapeSelectsStableKey(t *testing.T) {
	t.Parallel()
	order := metadataAddOrder(map[string]*commonpb.MetadataValue{
		"z": metadataStringValue("invalid\x00"),
		"a": metadataStringValue("invalid\x00"),
	})
	for range 100 {
		var keyErr *domain.ErrMetadataKeyValidation
		require.ErrorAs(t, validateOrderMetadata(order), &keyErr)
		require.Equal(t, "a", keyErr.Key)
		require.ErrorIs(t, keyErr, domain.ErrMetadataValueContainsNullByte)
	}
}
