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
		PeriodId:          7,
		PostCommitVolumes: &PostCommitVolumes{},
		PreviousAccountMetadata: map[string]*MetadataMap{
			"users:alice": {Values: map[string]*MetadataValue{
				"vip": NewStringValue("no"),
			}},
		},
	}

	data, err := ct.MarshalJSON()
	require.NoError(t, err)

	out := string(data)
	require.Contains(t, out, `"transaction":`)
	require.Contains(t, out, `"accountMetadata":`)
	require.Contains(t, out, `"periodId":7`)
	require.Contains(t, out, `"postCommitVolumes":`)
	require.Contains(t, out, `"previousAccountMetadata":`)
	require.False(t, strings.Contains(out, "period_id"), "must use camelCase")
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
