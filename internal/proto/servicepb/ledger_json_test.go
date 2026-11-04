package servicepb

import (
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
	require.JSONEq(t, `{"ledger":"main","cursor":9007199254740993,"currentVersion":2,"pendingVersion":0}`, string(raw))
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
