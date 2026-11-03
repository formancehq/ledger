package servicepb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestGeneratedMarshalJSONUsesCamelCaseAndNumericUint64(t *testing.T) {
	raw, err := json.Marshal(&IndexEntry{
		Ledger:         "main",
		Cursor:         9007199254740993,
		CurrentVersion: 2,
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"ledger":"main","cursor":9007199254740993,"currentVersion":2}`, string(raw))
}
