package v2

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

// The fixture is encoded by the real v2.4.7 logs.data writer's types, not
// V2RevertedTransactionData. See testdata/README.md for its provenance.
func TestTranslateBatch_RevertedTransactionSourcePayload(t *testing.T) {
	t.Parallel()

	data, err := os.ReadFile("testdata/v2.4.7-reverted-transaction-1.json")
	require.NoError(t, err)
	orders, nextLog, nextTx, err := TranslateBatch("default", []V2Log{{
		ID: 3, Type: "REVERTED_TRANSACTION", Date: "2024-01-01T01:00:00Z", Data: data,
	}}, 3, 2, nil)
	require.NoError(t, err)
	require.Len(t, orders, 1)
	rt := orders[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetRevertedTransaction()
	require.NotNil(t, rt)
	require.Equal(t, uint64(1), rt.GetRevertedTransactionId(), "original identity comes from logs.data.revertedTransaction.id")
	require.Equal(t, uint64(2), rt.GetNewTransactionId())
	require.Equal(t, uint64(4), nextLog)
	require.Equal(t, uint64(3), nextTx)
	require.Len(t, rt.GetReversePostings(), 1)
	require.Equal(t, "alice", rt.GetReversePostings()[0].GetSource())
	require.Equal(t, "world", rt.GetReversePostings()[0].GetDestination())
	require.Equal(t, "USD", rt.GetReversePostings()[0].GetAsset())
	require.Equal(t, "100", rt.GetReversePostings()[0].GetAmount().Dec())
}

func TestTranslateBatch_RevertedTransactionRequiresOriginalIdentity(t *testing.T) {
	t.Parallel()

	for _, data := range []string{
		`{"transaction":{"id":2}}`,
		`{"revertedTransactionID":1,"transaction":{"id":2}}`,
		`{"revertedTransaction":null,"transaction":{"id":2}}`,
		`{"revertedTransaction":{},"transaction":{"id":2}}`,
		`{"revertedTransaction":{"id":null},"transaction":{"id":2}}`,
	} {
		t.Run(data, func(t *testing.T) {
			t.Parallel()
			orders, nextLog, nextTx, err := TranslateBatch("default", []V2Log{{
				ID: 3, Type: "REVERTED_TRANSACTION", Data: []byte(data),
			}}, 3, 2, nil)
			require.ErrorContains(t, err, "REVERTED_TRANSACTION data is missing revertedTransaction.id")
			require.Nil(t, orders)
			require.Zero(t, nextLog)
			require.Zero(t, nextTx)

			// A failed decode must not poison a later attempt. Explicit ID zero
			// is valid, even if a conflicting memento field is also supplied.
			orders, nextLog, nextTx, err = TranslateBatch("default", []V2Log{{
				ID: 3, Type: "REVERTED_TRANSACTION", Date: "2024-01-01T01:00:00Z",
				Data: []byte(`{"revertedTransaction":{"id":0},"revertedTransactionID":99,"transaction":{"id":2}}`),
			}}, 3, 2, nil)
			require.NoError(t, err)
			require.Len(t, orders, 1)
			rt := orders[0].GetLedgerScoped().GetMirrorIngest().GetEntry().GetRevertedTransaction()
			require.NotNil(t, rt)
			require.Zero(t, rt.GetRevertedTransactionId())
			require.Equal(t, uint64(2), rt.GetNewTransactionId())
			require.Equal(t, uint64(4), nextLog)
			require.Equal(t, uint64(3), nextTx)
		})
	}
}

func TestResetV2RevertDataClearsOriginalIdentity(t *testing.T) {
	t.Parallel()

	data := &V2RevertedTransactionData{RevertedTransaction: V2TransactionIdentity{ID: new(uint64(1))}}
	resetV2RevertData(data)
	require.Nil(t, data.RevertedTransaction.ID, "a pooled decoder must not reuse a previous original ID")
}
