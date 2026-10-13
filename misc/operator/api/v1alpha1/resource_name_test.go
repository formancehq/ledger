package v1alpha1

import (
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"sigs.k8s.io/yaml"
)

// TestResourceNameCRDValidation pins the admission contract of
// spec.resourceName in the generated (and Helm-synced) CRDs: DNS-1035 label,
// at most 52 chars (StatefulSet controller-revision-hash label limit),
// immutable once set, and impossible to add or remove after creation. The
// rules are exercised against a real API server by the integration-tagged
// TestClusterResourceNameCRDValidation.
func TestResourceNameCRDValidation(t *testing.T) {
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
			spec := crd.Spec.Versions[0].Schema.OpenAPIV3Schema.Properties["spec"]

			field, ok := spec.Properties["resourceName"]
			require.True(t, ok)
			require.Equal(t, "^[a-z]([-a-z0-9]*[a-z0-9])?$", field.Pattern)
			require.NotNil(t, field.MaxLength)
			require.EqualValues(t, 52, *field.MaxLength)
			require.Contains(t, field.XValidations, apiextensionsv1.ValidationRule{
				Rule: "self == oldSelf", Message: "resourceName is immutable",
			})

			require.Contains(t, spec.XValidations, apiextensionsv1.ValidationRule{
				Rule:    "has(self.resourceName) == has(oldSelf.resourceName)",
				Message: "resourceName can only be set at creation: it cannot be added or removed afterwards",
			})
		})
	}
}
