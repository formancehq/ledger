package ledgers

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestParseInitialIndexes(t *testing.T) {
	t.Parallel()
	ids, err := parseInitialIndexes([]string{"reference", "account-asset", "metadata:transaction:external:id"})
	require.NoError(t, err)
	require.Len(t, ids, 3)
	require.Equal(t, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE, ids[0].GetTxBuiltin())
	require.Equal(t, commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET, ids[1].GetAccountBuiltin())
	require.Equal(t, "external:id", ids[2].GetMetadata().GetKey())
	require.Equal(t, commonpb.TargetType_TARGET_TYPE_TRANSACTION, ids[2].GetMetadata().GetTarget())
}

func TestParseInitialIndexesRejectsInvalidDeclarations(t *testing.T) {
	t.Parallel()
	for _, entries := range [][]string{
		{"reference", "reference"}, {"unknown"}, {"metadata"},
		{"metadata:account:"}, {"metadata:ledger:role"}, {"metadata:unknown:role"}, {"reference:account:role"},
	} {
		t.Run(entries[0], func(t *testing.T) {
			t.Parallel()
			_, err := parseInitialIndexes(entries)
			require.Error(t, err)
		})
	}
}
