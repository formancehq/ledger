package create

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestBuildClusterInheritsOperatorImageTag(t *testing.T) {
	t.Parallel()

	cluster, overrides, err := buildCluster("my-ledger", "default", nil)
	require.NoError(t, err)
	require.Equal(t, "ghcr.io/formancehq/ledger", cluster.Spec.Image.Repository)
	require.Empty(t, cluster.Spec.Image.Tag)
	require.NotContains(t, overrides["image"].(map[string]any), "tag")

	data, err := json.Marshal(cluster)
	require.NoError(t, err)
	require.NotContains(t, string(data), `"tag"`)
}

func TestBuildClusterKeepsExplicitImageTag(t *testing.T) {
	t.Parallel()

	cluster, _, err := buildCluster("my-ledger", "default", []string{"image.tag=v3.0.0-rc.3"})
	require.NoError(t, err)
	require.Equal(t, "v3.0.0-rc.3", cluster.Spec.Image.Tag)
}
