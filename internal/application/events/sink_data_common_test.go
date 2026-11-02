package events

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestSinkPopulateApply_DeletedAccountMetadata(t *testing.T) {
	t.Parallel()

	data := &sinkEventData{}
	sinkPopulateApply(data, &commonpb.ApplyLedgerLog{
		Log: &commonpb.LedgerLog{
			Data: &commonpb.LedgerLogPayload{
				Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
					CreatedTransaction: &commonpb.CreatedTransaction{
						Transaction: &commonpb.Transaction{Id: 1},
						DeletedAccountMetadata: map[string]*commonpb.MetadataKeys{
							"users:001": {Keys: []string{"a", "b"}},
						},
					},
				},
			},
		},
	})
	require.Equal(t, map[string][]string{"users:001": {"a", "b"}}, data.DeletedAccountMetadata)

	encoded, err := json.Marshal(data)
	require.NoError(t, err)
	require.Contains(t, string(encoded), `"deletedAccountMetadata":{"users:001":["a","b"]}`)
}
