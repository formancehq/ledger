package commonpb

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestPostCommitVolumes_MarshalJSON_Flat guards against the protojson leak
// reported on v3.0.0-alpha.7: the wire used to emit
// `{"volumesByAccount": {"addr": {"volumes": {"asset": ...}}}}` — two proto
// wrappers deep — while the OpenAPI schema documents a flat
// `{"addr": {"asset": ...}}` map. The MarshalJSON shim must flatten.
func TestPostCommitVolumes_MarshalJSON_Flat(t *testing.T) {
	t.Parallel()

	pcv := &PostCommitVolumes{
		VolumesByAccount: map[string]*VolumesByAssets{
			"users:alice": {Volumes: map[string]*Volumes{
				"USD/2": {Input: "100", Output: "40"},
			}},
			"world": {Volumes: map[string]*Volumes{
				"USD/2": {Input: "0", Output: "100"},
			}},
		},
	}

	data, err := pcv.MarshalJSON()
	require.NoError(t, err)

	var out map[string]map[string]struct {
		Input  string `json:"input"`
		Output string `json:"output"`
	}

	require.NoError(t, json.Unmarshal(data, &out))
	require.Len(t, out, 2)
	require.Equal(t, "100", out["users:alice"]["USD/2"].Input)
	require.Equal(t, "40", out["users:alice"]["USD/2"].Output)
	require.Equal(t, "0", out["world"]["USD/2"].Input)
	require.Equal(t, "100", out["world"]["USD/2"].Output)

	// Confirm the wrapper keys are absent from the wire.
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
			"volumesByAccount": {Volumes: map[string]*Volumes{
				"USD/2": {Input: "100", Output: "40"},
			}},
		},
	}

	data, err := pcv.MarshalJSON()
	require.NoError(t, err)

	var out map[string]map[string]struct {
		Input  string `json:"input"`
		Output string `json:"output"`
	}

	require.NoError(t, json.Unmarshal(data, &out))
	require.Contains(t, out, "volumesByAccount")
	require.Equal(t, "100", out["volumesByAccount"]["USD/2"].Input)
	require.Equal(t, "40", out["volumesByAccount"]["USD/2"].Output)
}
