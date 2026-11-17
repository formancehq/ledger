package create

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/misc/operator/cmd/kubectl-ledger/cmdutil"
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

func TestCreateDryRunRejectsInvalidSetPath(t *testing.T) {
	t.Parallel()

	for _, path := range []string{"ingress.tls[-1].secretName", "ingress.tls[65537].secretName", "ingress.tls[0]suffix"} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()

			opts := &cmdutil.Options{}
			root := &cobra.Command{SilenceErrors: true, SilenceUsage: true}
			opts.AddFlags(root)
			root.AddCommand(NewCommand(opts))
			root.SetArgs([]string{
				"--namespace", "default", "--kubeconfig", os.DevNull,
				"create", "audit", "--dry-run", "--set", path + "=value",
			})

			err := root.ExecuteContext(t.Context())
			require.ErrorContains(t, err, "--set")
			require.Contains(t, err.Error(), path)
		})
	}
}
