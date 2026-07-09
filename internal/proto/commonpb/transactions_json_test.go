package commonpb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCreatedTransaction_MarshalJSON_AllFields guards against the regression
// where the shim only emitted `transaction` and `accountMetadata` and dropped
// every other field. Clients asking for expandVolumes=true got a successful
// REST response with no postCommitVolumes — the value moved through gRPC fine
// but disappeared at the HTTP boundary (companion to #452).
func TestCreatedTransaction_MarshalJSON_AllFields(t *testing.T) {
	t.Parallel()

	ct := &CreatedTransaction{
		Transaction: &Transaction{Id: 1},
		AccountMetadata: map[string]*MetadataMap{
			"users:alice": {Values: map[string]*MetadataValue{
				"vip": NewStringValue("yes"),
			}},
		},
		ChapterId:         7,
		PostCommitVolumes: &PostCommitVolumes{},
	}

	data, err := ct.MarshalJSON()
	require.NoError(t, err)

	out := string(data)
	require.Contains(t, out, `"transaction":`)
	require.Contains(t, out, `"accountMetadata":`)
	require.Contains(t, out, `"chapterId":7`)
	require.Contains(t, out, `"postCommitVolumes":`)
	require.False(t, strings.Contains(out, "previousAccountMetadata"), "previous_account_metadata is no longer emitted")
	require.False(t, strings.Contains(out, "chapter_id"), "must use camelCase")
}

// TestRevertedTransaction_MarshalJSON_AllFields covers the same regression on
// the revert path and pins the casing to `revertedTransactionId` (Go-style
// `Id`, matching the proto3 JSON name).
func TestRevertedTransaction_MarshalJSON_AllFields(t *testing.T) {
	t.Parallel()

	rt := &RevertedTransaction{
		RevertedTransactionId: 42,
		RevertTransaction:     &Transaction{Id: 43},
		PostCommitVolumes:     &PostCommitVolumes{},
	}

	data, err := rt.MarshalJSON()
	require.NoError(t, err)

	out := string(data)
	require.Contains(t, out, `"revertedTransactionId":42`)
	require.Contains(t, out, `"revertTransaction":`)
	require.Contains(t, out, `"postCommitVolumes":`)
	require.False(t, strings.Contains(out, "revertedTransactionID"),
		"casing must follow the Go convention (Id, not ID)")
}

// TestTransaction_MarshalJSON_RevertRelationship pins the first-class revert
// relationship fields: revertedByTransactionId + revertedAt on the reverted
// original, revertsTransactionId on the compensating transaction. Casing follows
// the Go convention (Id, not ID) and unset links are omitted.
func TestTransaction_MarshalJSON_RevertRelationship(t *testing.T) {
	t.Parallel()

	t.Run("reverted original", func(t *testing.T) {
		t.Parallel()

		data, err := (&Transaction{
			Id:                    1,
			Reverted:              true,
			RevertedByTransaction: 2,
			RevertedAt:            &Timestamp{Data: 1_700_000_000_000_000},
		}).MarshalJSON()
		require.NoError(t, err)

		out := string(data)
		require.Contains(t, out, `"reverted":true`)
		require.Contains(t, out, `"revertedByTransactionId":2`)
		require.Contains(t, out, `"revertedAt":`)
		require.NotContains(t, out, "revertsTransactionId")
		require.NotContains(t, out, "revertedByTransactionID", "casing must follow the Go convention (Id, not ID)")
	})

	t.Run("compensating transaction", func(t *testing.T) {
		t.Parallel()

		data, err := (&Transaction{Id: 2, RevertsTransaction: 1}).MarshalJSON()
		require.NoError(t, err)

		out := string(data)
		require.Contains(t, out, `"revertsTransactionId":1`)
		require.Contains(t, out, `"reverted":false`)
		require.NotContains(t, out, "revertedByTransactionId")
		require.NotContains(t, out, "revertedAt")
	})

	t.Run("plain transaction omits revert links", func(t *testing.T) {
		t.Parallel()

		data, err := (&Transaction{Id: 3}).MarshalJSON()
		require.NoError(t, err)

		out := string(data)
		require.NotContains(t, out, "revertedByTransactionId")
		require.NotContains(t, out, "revertsTransactionId")
		require.NotContains(t, out, "revertedAt")
	})
}
