package server

import (
	"context"
	"os"
	"os/exec"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/stretchr/testify/require"

	otlp "github.com/formancehq/go-libs/v5/pkg/observe"
	"github.com/formancehq/go-libs/v5/pkg/service"

	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

func TestAddPyroscopeResourceTags(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		attributes string
		tags       map[string]string
		want       map[string]string
	}{
		{
			name:       "operator identity",
			attributes: "formance.ledger.cluster.name=payments,k8s.namespace.name=prod,k8s.pod.name=ledger-payments-1",
			tags:       map[string]string{"env": "staging"},
			want: map[string]string{
				"env":                          "staging",
				"k8s_namespace_name":           "prod",
				"formance_ledger_cluster_name": "payments",
				"formance_ledger_node_id":      "42",
			},
		},
		{
			name:       "resource overrides explicit tag",
			attributes: "formance.ledger.cluster.name=payments",
			tags:       map[string]string{"formance_ledger_cluster_name": "stale"},
			want: map[string]string{
				"formance_ledger_cluster_name": "payments",
				"formance_ledger_node_id":      "42",
			},
		},
		{
			// Without the operator: no namespace, and the cluster name
			// falls back to the cluster ID as on metrics.
			name:       "server defaults only",
			attributes: "deployment.environment=regression",
			tags:       map[string]string{"node_id": "2"},
			want: map[string]string{
				"node_id":                      "2",
				"formance_ledger_cluster_name": "cluster-a",
				"formance_ledger_node_id":      "42",
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cmd := &cobra.Command{}
			otlp.AddFlags(cmd.Flags())
			require.NoError(t, cmd.Flags().Set(otlp.OtelResourceAttributesFlag, test.attributes))
			res, err := resourceFromFlags(cmd, "cluster-a", 42, version.Info{})
			require.NoError(t, err)

			addPyroscopeResourceTags(test.tags, res)
			require.Equal(t, test.want, test.tags)
		})
	}
}

const pyroscopeEnvTagsHelperEnv = "LEDGER_PYROSCOPE_ENV_TAGS_HELPER"
const pyroscopeEnvTagsSentinel = "PYROSCOPE_ENV_TAGS_OK"

// The operator injects the identity through OTEL_RESOURCE_ATTRIBUTES, not the
// flag. service.Execute binds that variable to --otel-resource-attributes,
// which is what lets it override the server's identity defaults. The OTel SDK
// also caches the environment-derived default resource for the whole process,
// so a helper process gets a fresh read of the variable.
func TestAddPyroscopeResourceTagsFromEnvironment(t *testing.T) {
	if os.Getenv(pyroscopeEnvTagsHelperEnv) != "" {
		cmd := &cobra.Command{}
		otlp.AddFlags(cmd.Flags())
		require.NoError(t, service.BindEnvToCommandWithError(cmd))
		res, err := resourceFromFlags(cmd, "default", 42, version.Info{})
		require.NoError(t, err)

		tags := map[string]string{}
		addPyroscopeResourceTags(tags, res)
		require.Equal(t, map[string]string{
			"k8s_namespace_name":           "prod",
			"formance_ledger_cluster_name": "payments",
			"formance_ledger_node_id":      "42",
		}, tags)
		t.Log(pyroscopeEnvTagsSentinel)

		return
	}
	t.Parallel()

	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestAddPyroscopeResourceTagsFromEnvironment$", "-test.v")
	command.Env = append(os.Environ(),
		pyroscopeEnvTagsHelperEnv+"=1",
		"OTEL_RESOURCE_ATTRIBUTES=formance.ledger.cluster.name=payments,k8s.namespace.name=prod,k8s.pod.name=ledger-payments-1",
	)
	output, err := command.CombinedOutput()
	require.NoError(t, err, string(output))
	require.Contains(t, string(output), pyroscopeEnvTagsSentinel, "helper process did not run the environment assertion")
}
