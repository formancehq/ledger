package servicepb

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRevertTransactionPayload_MarshalJSON_Identifier guards the regression
// where MarshalJSON dropped the transactionReference oneof variant and only
// ever emitted transactionId.
func TestRevertTransactionPayload_MarshalJSON_Identifier(t *testing.T) {
	t.Parallel()

	t.Run("reference variant", func(t *testing.T) {
		t.Parallel()

		payload := &RevertTransactionPayload{
			Identifier: &RevertTransactionPayload_TransactionReference{
				TransactionReference: "order-123",
			},
			Force: true,
		}

		data, err := payload.MarshalJSON()
		require.NoError(t, err)

		out := string(data)
		require.Contains(t, out, `"transactionReference":"order-123"`)
		require.NotContains(t, out, "transactionId")
	})

	t.Run("id variant", func(t *testing.T) {
		t.Parallel()

		payload := &RevertTransactionPayload{
			Identifier: &RevertTransactionPayload_TransactionId{TransactionId: 42},
		}

		data, err := payload.MarshalJSON()
		require.NoError(t, err)

		out := string(data)
		require.Contains(t, out, `"transactionId":42`)
		require.False(t, strings.Contains(out, "transactionReference"),
			"id variant must not emit a reference key")
	})

	t.Run("expandVolumes and receipt are emitted", func(t *testing.T) {
		t.Parallel()

		payload := &RevertTransactionPayload{
			Identifier:    &RevertTransactionPayload_TransactionId{TransactionId: 1},
			ExpandVolumes: true,
			Receipt:       "rcpt-xyz",
		}

		data, err := payload.MarshalJSON()
		require.NoError(t, err)

		out := string(data)
		require.Contains(t, out, `"expandVolumes":true`)
		require.Contains(t, out, `"receipt":"rcpt-xyz"`)
	})
}
