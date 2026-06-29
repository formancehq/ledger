package commonpb

import (
	"encoding/json"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPosting_MarshalJSON_EmitsEmptyColor guards against the regression where
// REST transaction/create/revert responses omit `color` for the uncolored
// bucket because the generated struct tag is `json:"color,omitempty"`. The
// REST layer must surface `color: ""` so clients can distinguish "uncolored"
// from "field absent in an older response shape".
func TestPosting_MarshalJSON_EmitsEmptyColor(t *testing.T) {
	t.Parallel()

	p := NewPosting("world", "users:alice", "USD/2", big.NewInt(100))

	data, err := json.Marshal(p)
	require.NoError(t, err)
	require.Contains(t, string(data), `"color":""`,
		"uncolored postings must surface color:\"\" rather than dropping the field")
}

func TestPosting_MarshalJSON_EmitsColor(t *testing.T) {
	t.Parallel()

	p := NewColoredPosting("world", "users:alice", "USD/2", "GRANTS", big.NewInt(100))

	data, err := json.Marshal(p)
	require.NoError(t, err)
	require.Contains(t, string(data), `"color":"GRANTS"`)
}
