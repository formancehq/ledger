package domain

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestValidateOrderMetadataMirrorVariants(t *testing.T) {
	t.Parallel()
	invalid := map[string]*ledgerpb.MetadataValue{"k": ledgerpb.NewStringValue("bad\x00value")}
	for name, entry := range map[string]*raftcmdpb.MirrorLogEntry{
		"created transaction":  {Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{Metadata: invalid}}},
		"created account":      {Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{AccountMetadata: map[string]*ledgerpb.MetadataMap{"account": {Values: invalid}}}}},
		"reverted transaction": {Data: &raftcmdpb.MirrorLogEntry_RevertedTransaction{RevertedTransaction: &raftcmdpb.MirrorRevertedTransaction{Metadata: invalid}}},
		"saved":                {Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{SavedMetadata: &raftcmdpb.MirrorSavedMetadata{Metadata: invalid}}},
		"deleted":              {Data: &raftcmdpb.MirrorLogEntry_DeletedMetadata{DeletedMetadata: &raftcmdpb.MirrorDeletedMetadata{Key: "bad\x00key"}}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: entry}}}}}
			before := order.CloneVT()
			err := ValidateOrderMetadata(order, DefaultMetadataLimits)
			if name == "deleted" {
				require.ErrorIs(t, err, ErrMetadataKeyInvalidChar)
			} else {
				require.ErrorIs(t, err, ErrMetadataValueContainsNullByte)
			}
			require.True(t, before.EqualVT(order))
		})
	}
}

func TestValidateOrderMetadataDeterministicAccountAndKey(t *testing.T) {
	t.Parallel()
	metadata := map[string]*ledgerpb.MetadataMap{
		"z": {Values: map[string]*ledgerpb.MetadataValue{"z": ledgerpb.NewStringValue("\x00")}},
		"a": {Values: map[string]*ledgerpb.MetadataValue{"z": ledgerpb.NewStringValue("\x00"), "a": ledgerpb.NewStringValue("\x00")}},
	}
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{AccountMetadata: metadata}}}}}}}}
	for range 50 {
		err := ValidateOrderMetadata(order, DefaultMetadataLimits)
		var account *ErrAccountValidation
		require.ErrorAs(t, err, &account)
		require.Equal(t, "a", account.Account)
		require.Equal(t, "a", FailureFactsOf(err).Facts["key"])
	}
}

func TestValidateCommandMetadataCountsMapsAndBareKeys(t *testing.T) {
	t.Parallel()
	orders := []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{Metadata: map[string]*ledgerpb.MetadataValue{"key": ledgerpb.NewStringValue("value")}}}}}},
		{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{Data: &raftcmdpb.MirrorLogEntry_DeletedMetadata{DeletedMetadata: &raftcmdpb.MirrorDeletedMetadata{Key: "bare"}}}}}}}},
	}
	limits := MetadataLimits{MaxEntriesPerEntity: 2, MaxKeyBytes: 4, MaxValueBytes: 5, MaxTotalBytesPerEntity: 8, MaxTotalBytesPerCommand: 12}
	require.NoError(t, ValidateCommandMetadata(orders, limits))
	limits.MaxTotalBytesPerCommand = 11
	for _, order := range orders {
		require.NoError(t, ValidateCommandMetadata([]*raftcmdpb.Order{order}, limits))
	}
	var limitErr *ErrMetadataLimitExceeded
	require.ErrorAs(t, ValidateCommandMetadata(orders, limits), &limitErr)
	require.Equal(t, MetadataLimitDimensionCommand, limitErr.Dimension)
	require.EqualValues(t, 12, limitErr.Actual)
}

func TestCreateLedgerMetadataValidation(t *testing.T) {
	t.Parallel()
	limits := MetadataLimits{MaxEntriesPerEntity: 2, MaxKeyBytes: 8, MaxValueBytes: 16, MaxTotalBytesPerEntity: 40, MaxTotalBytesPerCommand: 60}
	create := func(m map[string]*ledgerpb.MetadataValue) *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "main", Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Metadata: m}}}}}
	}
	for _, tc := range []struct {
		name      string
		metadata  map[string]*ledgerpb.MetadataValue
		dimension string
		invalid   bool
	}{
		{name: "omitted"},
		{name: "supported null", metadata: map[string]*ledgerpb.MetadataValue{"k": ledgerpb.NewNullValue("original")}},
		{name: "empty key", metadata: map[string]*ledgerpb.MetadataValue{"": ledgerpb.NewStringValue("v")}, invalid: true},
		{name: "invalid key", metadata: map[string]*ledgerpb.MetadataValue{"bad%key": ledgerpb.NewStringValue("v")}, invalid: true},
		{name: "invalid value", metadata: map[string]*ledgerpb.MetadataValue{"k": ledgerpb.NewStringValue("\x00")}, invalid: true},
		{name: "empty value follows existing metadata contract", metadata: map[string]*ledgerpb.MetadataValue{"k": {}}},
		{name: "entries", metadata: map[string]*ledgerpb.MetadataValue{"a": ledgerpb.NewStringValue("v"), "b": ledgerpb.NewStringValue("v"), "c": ledgerpb.NewStringValue("v")}, dimension: MetadataLimitDimensionEntries},
		{name: "key", metadata: map[string]*ledgerpb.MetadataValue{"123456789": ledgerpb.NewStringValue("v")}, dimension: MetadataLimitDimensionKey},
		{name: "value", metadata: map[string]*ledgerpb.MetadataValue{"k": ledgerpb.NewStringValue("12345678901234567")}, dimension: MetadataLimitDimensionValue},
		{name: "entity", metadata: map[string]*ledgerpb.MetadataValue{"12345678": ledgerpb.NewStringValue("1234567890123456"), "abcdefgh": ledgerpb.NewStringValue("1234567890123456")}, dimension: MetadataLimitDimensionEntity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			order := create(tc.metadata)
			before := order.CloneVT()
			err := ValidateCommandMetadata([]*raftcmdpb.Order{order}, limits)
			require.True(t, before.EqualVT(order))
			switch {
			case tc.dimension != "":
				var limitErr *ErrMetadataLimitExceeded
				require.ErrorAs(t, err, &limitErr)
				require.Equal(t, tc.dimension, limitErr.Dimension)
			case tc.invalid:
				require.Error(t, err)
			default:
				require.NoError(t, err)
			}
		})
	}
	metadata := map[string]*ledgerpb.MetadataValue{"12345678": ledgerpb.NewStringValue("1234567890123456"), "abcdefgh": ledgerpb.NewStringValue("12345678")}
	first, second := create(metadata), create(metadata)
	require.EqualValues(t, 40, OrderMetadataSize(first))
	require.NoError(t, ValidateCommandMetadata([]*raftcmdpb.Order{first}, limits))
	var limitErr *ErrMetadataLimitExceeded
	require.ErrorAs(t, ValidateCommandMetadata([]*raftcmdpb.Order{first, second}, limits), &limitErr)
	require.Equal(t, MetadataLimitDimensionCommand, limitErr.Dimension)
	require.EqualValues(t, 80, limitErr.Actual)
}
