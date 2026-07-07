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

// TestPostCommitVolumes_UnmarshalJSON_FlatRoundTrip confirms the flat wire
// shape round-trips cleanly through Marshal → Unmarshal.
func TestPostCommitVolumes_UnmarshalJSON_FlatRoundTrip(t *testing.T) {
	t.Parallel()

	original := &PostCommitVolumes{
		VolumesByAccount: map[string]*VolumesByAssets{
			"users:alice": {Volumes: map[string]*Volumes{
				"USD/2": {Input: "100", Output: "40"},
			}},
		},
	}

	data, err := original.MarshalJSON()
	require.NoError(t, err)

	var out PostCommitVolumes
	require.NoError(t, out.UnmarshalJSON(data))

	require.Equal(t, "100", out.VolumesByAccount["users:alice"].Volumes["USD/2"].Input)
	require.Equal(t, "40", out.VolumesByAccount["users:alice"].Volumes["USD/2"].Output)
}

// TestPostCommitVolumes_UnmarshalJSON_LegacyWrappedRejected asserts we
// reject the pre-EN-1465 wrapped shape with an explicit error instead of
// silently mis-parsing it. Callers migrating stored alpha responses will
// therefore see a clear failure rather than zero-valued Volumes maps.
func TestPostCommitVolumes_UnmarshalJSON_LegacyWrappedRejected(t *testing.T) {
	t.Parallel()

	legacy := `{
		"volumesByAccount": {
			"users:alice": {
				"volumes": {
					"USD/2": {"input": "100", "output": "40"}
				}
			}
		}
	}`

	var out PostCommitVolumes

	err := out.UnmarshalJSON([]byte(legacy))
	require.Error(t, err)
	require.Contains(t, err.Error(), "pre-EN-1465 wrapped postCommitVolumes")
}

// TestPostCommitVolumes_UnmarshalJSON_AccountNamedVolumesByAccount asserts
// that an account literally named `volumesByAccount` (a legal account
// address) round-trips through the flat encoder without being mistaken for
// the pre-EN-1465 wrapped shape. Legacy-shape detection has to be
// structural, not based on the top-level key alone.
func TestPostCommitVolumes_UnmarshalJSON_AccountNamedVolumesByAccount(t *testing.T) {
	t.Parallel()

	original := &PostCommitVolumes{
		VolumesByAccount: map[string]*VolumesByAssets{
			"volumesByAccount": {Volumes: map[string]*Volumes{
				"USD/2": {Input: "100", Output: "40"},
			}},
		},
	}

	data, err := original.MarshalJSON()
	require.NoError(t, err)

	var out PostCommitVolumes
	require.NoError(t, out.UnmarshalJSON(data))

	require.Equal(t, "100", out.VolumesByAccount["volumesByAccount"].Volumes["USD/2"].Input)
	require.Equal(t, "40", out.VolumesByAccount["volumesByAccount"].Volumes["USD/2"].Output)
}
