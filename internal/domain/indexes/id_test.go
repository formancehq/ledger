package indexes_test

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestSupported(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		id   *commonpb.IndexID
		want bool
	}{
		{"metadata ACCOUNT", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "k"), true},
		{"metadata TRANSACTION", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "k"), true},
		{"metadata LEDGER", indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_LEDGER, "k"), false},
		{"tx builtin REFERENCE (enum 0)", indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE), true},
		{"tx builtin REVERTED_AT", indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT), true},
		{"tx builtin out-of-range enum", indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex(999)), false},
		{"account builtin ASSET", indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET), true},
		{"account builtin UNSPECIFIED", indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_UNSPECIFIED), false},
		{"log builtin DATE", indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE), true},
		{"log builtin UNSPECIFIED", indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_UNSPECIFIED), false},
		{"nil id", nil, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tt.want, indexes.Supported(tt.id))
		})
	}
}

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

func TestParseCanonical(t *testing.T) {
	t.Parallel()

	// Round-trip every kind through Canonical then ParseCanonical.
	roundTrip := []*commonpb.IndexID{
		indexes.TxBuiltinID(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE),
		indexes.LogBuiltinID(commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE),
		indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET),
		indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "color"),
		indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_TRANSACTION, "reference"),
	}

	for _, id := range roundTrip {
		s := indexes.Canonical(id)
		got, err := indexes.ParseCanonical(s)
		require.NoError(t, err, "ParseCanonical(%q)", s)
		assert.True(t, indexes.Equal(id, got), "round-trip mismatch for %q", s)
	}

	// Error cases.
	errCases := []string{
		"",
		"tx_builtin",                    // missing prefix separator
		"unknown:X",                     // unknown prefix
		"tx_builtin:BOGUS",              // unknown enum
		"log_builtin:BOGUS",             // unknown enum
		"account_builtin:BOGUS",         // unknown enum
		"metadata:BOGUS:key",            // unknown target
		"metadata:TARGET_TYPE_ACCOUNT:", // empty key
		"metadata:TARGET_TYPE_ACCOUNT",  // missing key separator
	}

	for _, s := range errCases {
		_, err := indexes.ParseCanonical(s)
		require.Error(t, err, "expected error for %q", s)
		assert.True(t, errors.Is(err, indexes.ErrInvalidCanonical), "expected ErrInvalidCanonical for %q, got %v", s, err)
	}
}
