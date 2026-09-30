package grpc

import (
	"encoding/json"
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

	p := &Posting{Source: "world", Destination: "users:alice", Asset: "USD/2", Amount: &Uint256{V0: 100}}

	data, err := json.Marshal(p)
	require.NoError(t, err)
	require.Contains(t, string(data), `"color":""`,
		"uncolored postings must surface color:\"\" rather than dropping the field")
}

func TestPosting_MarshalJSON_EmitsColor(t *testing.T) {
	t.Parallel()

	p := &Posting{Source: "world", Destination: "users:alice", Asset: "USD/2", Color: "GRANTS", Amount: &Uint256{V0: 100}}

	data, err := json.Marshal(p)
	require.NoError(t, err)
	require.Contains(t, string(data), `"color":"GRANTS"`)
}
