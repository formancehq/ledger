package commonpb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestCreatedTransaction_MarshalJSON_AllFields guards against the regression
// where the shim only emitted `transaction` and `accountMetadata` and dropped
// every other field (#452). Post-commit volumes now ride on the embedded
// transaction, so the wrapper emits no sibling postCommitVolumes.
func TestCreatedTransaction_MarshalJSON_AllFields(t *testing.T) {
	t.Parallel()

	ct := &CreatedTransaction{
		Transaction: &Transaction{Id: 1, PostCommitVolumes: &PostCommitVolumes{}},
		AccountMetadata: map[string]*MetadataMap{
			"users:alice": {Values: map[string]*MetadataValue{
				"vip": NewStringValue("yes"),
			}},
		},
	}

	data, err := ct.MarshalJSON()
	require.NoError(t, err)

	out := string(data)
	require.Contains(t, out, `"transaction":`)
	require.Contains(t, out, `"accountMetadata":`)
	// postCommitVolumes rides on the transaction, so it appears after the
	// "transaction": key, not as a top-level sibling.
	require.Contains(t, out, `"postCommitVolumes":`)
	require.Greater(t, strings.Index(out, `"postCommitVolumes":`), strings.Index(out, `"transaction":`),
		"postCommitVolumes must be nested under transaction, not a wrapper sibling")
	require.False(t, strings.Contains(out, "previousAccountMetadata"), "previous_account_metadata is no longer emitted")
}

// TestRevertedTransaction_MarshalJSON_AllFields covers the same regression on
// the revert path and pins the casing to `revertedTransactionId` (Go-style
// `Id`, matching the proto3 JSON name). Post-commit volumes ride on the
// embedded revert transaction.
func TestRevertedTransaction_MarshalJSON_AllFields(t *testing.T) {
	t.Parallel()

	rt := &RevertedTransaction{
		RevertedTransactionId: 42,
		RevertTransaction:     &Transaction{Id: 43, PostCommitVolumes: &PostCommitVolumes{}},
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

// TestTransaction_MarshalJSON_PostCommitVolumes pins the post-commit volume
// snapshot to the transaction itself: emitted (camelCase) when present, omitted
// when nil. The nested shape is one flat {asset, color, input, output} row per
// account, matching PostCommitVolumes.MarshalJSON.
func TestTransaction_MarshalJSON_PostCommitVolumes(t *testing.T) {
	t.Parallel()

	t.Run("present", func(t *testing.T) {
		t.Parallel()

		data, err := (&Transaction{
			Id: 1,
			PostCommitVolumes: &PostCommitVolumes{VolumesByAccount: map[string]*VolumesByAssets{
				"users:alice": {Volumes: []*VolumeEntry{
					{Asset: "USD/2", Color: "", Volumes: &Volumes{Input: "0", Output: "1000"}},
				}},
			}},
		}).MarshalJSON()
		require.NoError(t, err)

		out := string(data)
		require.Contains(t, out, `"postCommitVolumes":`)
		require.Contains(t, out, `"users:alice":`)
		require.Contains(t, out, `"asset":"USD/2"`)
		require.Contains(t, out, `"output":"1000"`)
		require.False(t, strings.Contains(out, "post_commit_volumes"), "must use camelCase")
	})

	t.Run("absent when nil", func(t *testing.T) {
		t.Parallel()

		data, err := (&Transaction{Id: 2}).MarshalJSON()
		require.NoError(t, err)

		require.NotContains(t, string(data), "postCommitVolumes")
	})
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

// TestTransaction_MarshalJSON_NoNullCollections pins the presence policy:
// collections are always emitted and never null, so the OpenAPI schema can
// declare `postings`/`metadata` as non-nullable and list them in `required`.
// `id` is required by TransactionResponse, so it is emitted unconditionally
// rather than relying on ids never being 0. Mirrors Account.MarshalJSON.
func TestTransaction_MarshalJSON_NoNullCollections(t *testing.T) {
	t.Parallel()

	data, err := (&Transaction{Id: 7}).MarshalJSON()
	require.NoError(t, err)

	out := string(data)

	require.NotContains(t, out, `"postings":null`)
	require.NotContains(t, out, `"metadata":null`)
	require.Contains(t, out, `"postings":[]`)
	require.Contains(t, out, `"metadata":{}`)
	require.Contains(t, out, `"id":7`)
}

// TestTransaction_MarshalJSON_ZeroIDStillEmitted guards the `required: [id]`
// claim in openapi.yml: a schema-required field must never be absent, even for
// the zero value that NextTransactionId should make unreachable.
func TestTransaction_MarshalJSON_ZeroIDStillEmitted(t *testing.T) {
	t.Parallel()

	data, err := (&Transaction{}).MarshalJSON()
	require.NoError(t, err)

	require.Contains(t, string(data), `"id":0`)
}
