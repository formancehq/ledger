package commonpb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// volumeEntryJSON is the decoded wire shape of a single post-commit volume
// tuple: `{asset, color, input, output}` — flat, with the color dimension
// carried explicitly (empty string = uncolored bucket).
type volumeEntryJSON struct {
	Asset  string `json:"asset"`
	Color  string `json:"color"`
	Input  string `json:"input"`
	Output string `json:"output"`
}

// TestPostCommitVolumes_MarshalJSON_Flat guards the wire contract chosen for
// the color-of-money model: the wire is a flat
// `{"addr": [{asset, color, input, output}]}` map — one array of (asset,
// color) tuples per account. protojson would otherwise emit the raw proto
// wrappers (`{"volumesByAccount": {"addr": {"volumes": [...]}}}`) two levels
// deep. This replaces the pre-color EN-1465 `{"addr": {"asset": Volumes}}`
// map shape, which can no longer key a bucket uniquely once a color dimension
// exists.
func TestPostCommitVolumes_MarshalJSON_Flat(t *testing.T) {
	t.Parallel()

	pcv := &PostCommitVolumes{
		VolumesByAccount: map[string]*VolumesByAssets{
			"users:alice": {Volumes: []*VolumeEntry{
				{Asset: "USD/2", Color: "", Volumes: &Volumes{Input: "100", Output: "40"}},
				{Asset: "USD/2", Color: "GOLD", Volumes: &Volumes{Input: "10", Output: "0"}},
			}},
			"world": {Volumes: []*VolumeEntry{
				{Asset: "USD/2", Color: "", Volumes: &Volumes{Input: "0", Output: "100"}},
			}},
		},
	}

	data, err := pcv.MarshalJSON()
	require.NoError(t, err)

	var out map[string][]volumeEntryJSON
	require.NoError(t, json.Unmarshal(data, &out))
	require.Len(t, out, 2)

	require.Len(t, out["users:alice"], 2)
	// Uncolored bucket.
	require.Equal(t, "USD/2", out["users:alice"][0].Asset)
	require.Equal(t, "", out["users:alice"][0].Color)
	require.Equal(t, "100", out["users:alice"][0].Input)
	require.Equal(t, "40", out["users:alice"][0].Output)
	// Colored bucket, distinct from the uncolored one.
	require.Equal(t, "USD/2", out["users:alice"][1].Asset)
	require.Equal(t, "GOLD", out["users:alice"][1].Color)
	require.Equal(t, "10", out["users:alice"][1].Input)
	require.Equal(t, "0", out["users:alice"][1].Output)

	require.Len(t, out["world"], 1)
	require.Equal(t, "", out["world"][0].Color)
	require.Equal(t, "0", out["world"][0].Input)
	require.Equal(t, "100", out["world"][0].Output)

	// Confirm the proto wrapper keys are absent from the wire.
	require.NotContains(t, string(data), "volumesByAccount")
	require.NotContains(t, string(data), `"volumes"`)
}

// TestPostCommitVolumes_MarshalJSON_Empty checks the empty-map case marshals
// to `{}` (not `null`) so downstream consumers can always dereference into a
// map without a nil-check.
func TestPostCommitVolumes_MarshalJSON_Empty(t *testing.T) {
	t.Parallel()

	data, err := (&PostCommitVolumes{}).MarshalJSON()
	require.NoError(t, err)
	require.Equal(t, "{}", string(data))
}

// TestPostCommitVolumes_MarshalJSON_AccountNamedVolumesByAccount asserts that
// an account literally named `volumesByAccount` (a legal address) round-trips
// through the flat encoder as any other account key — no top-level wrapping,
// no key collision with the pre-EN-1465 protobuf wrapper name.
func TestPostCommitVolumes_MarshalJSON_AccountNamedVolumesByAccount(t *testing.T) {
	t.Parallel()

	pcv := &PostCommitVolumes{
		VolumesByAccount: map[string]*VolumesByAssets{
			"volumesByAccount": {Volumes: []*VolumeEntry{
				{Asset: "USD/2", Color: "", Volumes: &Volumes{Input: "100", Output: "40"}},
			}},
		},
	}

	data, err := pcv.MarshalJSON()
	require.NoError(t, err)

	var out map[string][]volumeEntryJSON
	require.NoError(t, json.Unmarshal(data, &out))
	require.Contains(t, out, "volumesByAccount")
	require.Len(t, out["volumesByAccount"], 1)
	require.Equal(t, "USD/2", out["volumesByAccount"][0].Asset)
	require.Equal(t, "100", out["volumesByAccount"][0].Input)
	require.Equal(t, "40", out["volumesByAccount"][0].Output)
}
