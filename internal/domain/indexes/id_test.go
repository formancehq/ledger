package indexes_test

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestEqual(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		a, b *commonpb.IndexID
		want bool
	}{
		{"both nil", nil, nil, true},
		{"one nil", indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE), nil, false},
		{
			"same tx_builtin",
			indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
			indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
			true,
		},
		{
			"different tx_builtin",
			indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
			indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
			false,
		},
		{
			"same log_builtin",
			indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
			indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
			true,
		},
		{
			"same metadata",
			indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "color"),
			indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "color"),
			true,
		},
		{
			"metadata different target",
			indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "color"),
			indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "color"),
			false,
		},
		{
			"metadata different key",
			indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "color"),
			indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "shape"),
			false,
		},
		{
			"cross-kind",
			indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
			indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "any"),
			false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			assert.Equal(t, tt.want, indexes.Equal(tt.a, tt.b))
		})
	}
}

func TestCanonical(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "", indexes.Canonical(nil))
	assert.Equal(t,
		"tx_builtin:TX_BUILTIN_INDEX_REFERENCE",
		indexes.Canonical(indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)),
	)
	assert.Equal(t,
		"log_builtin:LOG_BUILTIN_INDEX_DATE",
		indexes.Canonical(indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE)),
	)
	assert.Equal(t,
		"metadata:TARGET_TYPE_ACCOUNT:color",
		indexes.Canonical(indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "color")),
	)

	// Canonical must collide iff Equal returns true.
	a := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "color")
	b := indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "color")
	assert.Equal(t, indexes.Canonical(a), indexes.Canonical(b))
}
