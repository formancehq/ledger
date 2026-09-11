package domain

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestValidateOrderMetadataMirrorVariants(t *testing.T) {
	t.Parallel()
	invalid := map[string]*commonpb.MetadataValue{"k": commonpb.NewStringValue("bad\x00value")}
	for name, entry := range map[string]*raftcmdpb.MirrorLogEntry{
		"created transaction":  {Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{Metadata: invalid}}},
		"created account":      {Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{AccountMetadata: map[string]*commonpb.MetadataMap{"account": {Values: invalid}}}}},
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
	metadata := map[string]*commonpb.MetadataMap{
		"z": {Values: map[string]*commonpb.MetadataValue{"z": commonpb.NewStringValue("\x00")}},
		"a": {Values: map[string]*commonpb.MetadataValue{"z": commonpb.NewStringValue("\x00"), "a": commonpb.NewStringValue("\x00")}},
	}
	order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: &raftcmdpb.MirrorLogEntry{Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{AccountMetadata: metadata}}}}}}}}
	for range 50 {
		err := ValidateOrderMetadata(order, DefaultMetadataLimits)
		var account *ErrAccountValidation
		require.ErrorAs(t, err, &account)
		require.Equal(t, "a", account.Account)
		var key *ErrMetadataKeyValidation
		require.ErrorAs(t, err, &key)
		require.Equal(t, "a", key.Key)
	}
}

func TestValidateCommandMetadataCountsMapsAndBareKeys(t *testing.T) {
	t.Parallel()
	orders := []*raftcmdpb.Order{
		{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{SaveLedgerMetadata: &raftcmdpb.SaveLedgerMetadataOrder{Metadata: map[string]*commonpb.MetadataValue{"key": commonpb.NewStringValue("value")}}}}}},
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
