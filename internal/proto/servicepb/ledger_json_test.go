package servicepb

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/stretchr/testify/require"
)

func TestGeneratedMarshalJSONUsesCamelCaseAndNumericUint64(t *testing.T) {
	raw, err := json.Marshal(&IndexEntry{
		Ledger:         "main",
		Cursor:         9007199254740993,
		CurrentVersion: 2,
	})
	require.NoError(t, err)
	var got struct {
		Ledger         string      `json:"ledger"`
		Cursor         json.Number `json:"cursor"`
		CurrentVersion uint64      `json:"currentVersion"`
		PendingVersion uint64      `json:"pendingVersion"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	require.NoError(t, decoder.Decode(&got))
	require.Equal(t, "main", got.Ledger)
	require.Equal(t, "9007199254740993", got.Cursor.String())
	require.EqualValues(t, 2, got.CurrentVersion)
	require.EqualValues(t, 0, got.PendingVersion)
}

func TestListNumscriptVersionsJSONProjectsNestedEntries(t *testing.T) {
	raw, err := json.Marshal(&ListNumscriptVersionsResponse{
		LatestVersion: "v2",
		Versions: []*commonpb.NumscriptVersionEntry{{
			Version:   "v1",
			CreatedAt: &commonpb.Timestamp{Data: 123},
		}},
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"latestVersion":"v2","versions":[{"version":"v1","createdAt":"1970-01-01T00:00:00.000123Z"}]}`, string(raw))
}
