package v1alpha1

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	apiextensions "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema"
	"k8s.io/apiextensions-apiserver/pkg/apiserver/schema/pruning"
	"sigs.k8s.io/yaml"
)

func TestPyroscopeCRDSecretBoundary(t *testing.T) {
	t.Parallel()
	for _, path := range []string{
		"../../config/crd/bases/ledger.formance.com_clusters.yaml",
		"../../helm/crds/templates/ledger.formance.com_clusters.yaml",
	} {
		t.Run(path, func(t *testing.T) {
			t.Parallel()
			data, err := os.ReadFile(path)
			require.NoError(t, err)
			var crd apiextensionsv1.CustomResourceDefinition
			require.NoError(t, yaml.Unmarshal(data, &crd))
			pyro := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"].Properties["monitoring"].Properties["pyroscope"]
			require.NotContains(t, pyro.Properties, "authToken")
			require.NotContains(t, pyro.Properties, "basicAuthPassword")
			var internal apiextensions.JSONSchemaProps
			require.NoError(t, apiextensionsv1.Convert_v1_JSONSchemaProps_To_apiextensions_JSONSchemaProps(&pyro, &internal, nil))
			structural, err := schema.NewStructural(&internal)
			require.NoError(t, err)
			obj := map[string]any{"enabled": true, "authToken": "AUDIT_TOKEN", "basicAuthPassword": "AUDIT_PASSWORD",
				"authTokenFrom":         map[string]any{"name": "profiling", "key": "token"},
				"basicAuthPasswordFrom": map[string]any{"name": "profiling", "key": "password"}}
			pruning.Prune(obj, structural, false)
			require.NotContains(t, obj, "authToken")
			require.NotContains(t, obj, "basicAuthPassword")
			require.Equal(t, true, obj["enabled"])
			require.Equal(t, map[string]any{"name": "profiling", "key": "token"}, obj["authTokenFrom"])
			require.Equal(t, map[string]any{"name": "profiling", "key": "password"}, obj["basicAuthPasswordFrom"])
		})
	}
}
