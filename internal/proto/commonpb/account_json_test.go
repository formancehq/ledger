package commonpb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestAccountVolume_MarshalJSON_EmitsEmptyColor guards the contract that
// REST responses serializing AccountVolume directly (e.g. via gRPC-Gateway
// or ad-hoc marshaling) emit color:"" for the uncolored bucket rather
// than dropping the field via the generated omitempty tag.
func TestAccountVolume_MarshalJSON_EmitsEmptyColor(t *testing.T) {
	t.Parallel()

	av := &AccountVolume{Asset: "USD/2"}

	data, err := json.Marshal(av)
	require.NoError(t, err)
	require.Contains(t, string(data), `"color":""`,
		"uncolored AccountVolume rows must surface color:\"\" rather than dropping the field")
}

func TestAccountVolume_MarshalJSON_EmitsColor(t *testing.T) {
	t.Parallel()

	av := &AccountVolume{Asset: "USD/2", Color: "GRANTS"}

	data, err := json.Marshal(av)
	require.NoError(t, err)
	require.Contains(t, string(data), `"color":"GRANTS"`)
}
