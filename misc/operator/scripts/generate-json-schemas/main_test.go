package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"github.com/stretchr/testify/require"
)

func TestRunGeneratesSchemasFromCRDs(t *testing.T) {
	t.Parallel()

	crdDir := filepath.Join("..", "..", "config", "crd", "bases")
	outDir := t.TempDir()

	require.NoError(t, run(crdDir, outDir))

	kindSchemas := []string{
		"ledger.formance.com_v1alpha1_backup.json",
		"ledger.formance.com_v1alpha1_backup.spec.json",
		"ledger.formance.com_v1alpha1_backuprun.json",
		"ledger.formance.com_v1alpha1_backuprun.spec.json",
		"ledger.formance.com_v1alpha1_cluster.json",
		"ledger.formance.com_v1alpha1_cluster.spec.json",
		"ledger.formance.com_v1alpha1_credentials.json",
		"ledger.formance.com_v1alpha1_credentials.spec.json",
		"ledger.formance.com_v1alpha1_eventsink.json",
		"ledger.formance.com_v1alpha1_eventsink.spec.json",
		"ledger.formance.com_v1alpha1_ledger.json",
		"ledger.formance.com_v1alpha1_ledger.spec.json",
	}
	dispatcherSchema := "ledger.formance.com.json"

	entries, err := os.ReadDir(outDir)
	require.NoError(t, err)

	actual := make([]string, 0, len(entries))
	for _, entry := range entries {
		actual = append(actual, entry.Name())
	}
	expected := append(append([]string{}, kindSchemas...), dispatcherSchema)
	require.ElementsMatch(t, expected, actual, "generated schema files must exactly match the expected set")

	for _, name := range kindSchemas {
		path := filepath.Join(outDir, name)

		data, err := os.ReadFile(path)
		require.NoError(t, err)

		var doc map[string]any
		require.NoError(t, json.Unmarshal(data, &doc))
		require.Equal(t, jsonSchemaDraft04, doc["$schema"])
		require.NotEmpty(t, doc["type"])
	}
}

func TestRunGeneratesDispatcherSchema(t *testing.T) {
	t.Parallel()

	crdDir := filepath.Join("..", "..", "config", "crd", "bases")
	outDir := t.TempDir()

	require.NoError(t, run(crdDir, outDir))

	data, err := os.ReadFile(filepath.Join(outDir, "ledger.formance.com.json"))
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(data, &doc))
	require.Equal(t, jsonSchemaDraft07, doc["$schema"])

	allOf, ok := doc["allOf"].([]any)
	require.True(t, ok)
	require.Len(t, allOf, 6, "expected one dispatch branch per CRD kind")

	refByKind := map[string]string{}
	for _, branch := range allOf {
		b, ok := branch.(map[string]any)
		require.True(t, ok)

		ifClause, ok := b["if"].(map[string]any)
		require.True(t, ok)

		props, ok := ifClause["properties"].(map[string]any)
		require.True(t, ok)

		apiVersion, ok := props["apiVersion"].(map[string]any)["const"].(string)
		require.True(t, ok)
		require.Equal(t, "ledger.formance.com/v1alpha1", apiVersion)

		kind, ok := props["kind"].(map[string]any)["const"].(string)
		require.True(t, ok)

		then, ok := b["then"].(map[string]any)
		require.True(t, ok)

		ref, ok := then["$ref"].(string)
		require.True(t, ok)

		refByKind[kind] = ref
	}

	require.Equal(t, map[string]string{
		"Backup":      "ledger.formance.com_v1alpha1_backup.json",
		"BackupRun":   "ledger.formance.com_v1alpha1_backuprun.json",
		"Cluster":     "ledger.formance.com_v1alpha1_cluster.json",
		"Credentials": "ledger.formance.com_v1alpha1_credentials.json",
		"EventSink":   "ledger.formance.com_v1alpha1_eventsink.json",
		"Ledger":      "ledger.formance.com_v1alpha1_ledger.json",
	}, refByKind)
}

func TestRunGeneratesStrictSchemas(t *testing.T) {
	t.Parallel()

	crdDir := filepath.Join("..", "..", "config", "crd", "bases")
	outDir := t.TempDir()

	require.NoError(t, run(crdDir, outDir))

	data, err := os.ReadFile(filepath.Join(outDir, "ledger.formance.com_v1alpha1_cluster.spec.json"))
	require.NoError(t, err)

	var doc map[string]any
	require.NoError(t, json.Unmarshal(data, &doc))

	require.Equal(t, false, doc["additionalProperties"], "root spec object must reject unknown properties")

	properties, ok := doc["properties"].(map[string]any)
	require.True(t, ok)

	bloom, ok := properties["bloom"].(map[string]any)
	require.True(t, ok, "expected a bloom object property")
	require.Equal(t, false, bloom["additionalProperties"], "nested object with a fixed property set must be strict")

	additionalLabels, ok := properties["additionalLabels"].(map[string]any)
	require.True(t, ok, "expected an additionalLabels map property")
	require.Equal(t,
		map[string]any{"type": "string"},
		additionalLabels["additionalProperties"],
		"a map-type field's value schema must not be overwritten with false",
	)
}

// TestRunRequiresResourceIdentity compiles the generated full-resource schema
// with a real JSON Schema validator and checks it actually rejects documents
// with a missing or wrong apiVersion/kind, and accepts a correctly-identified
// one. A structural assertion on "required"/"enum" isn't enough on its own:
// this is what makes the fix observable the same way the review finding that
// prompted it was (compiling the schema and validating sample documents).
func TestRunRequiresResourceIdentity(t *testing.T) {
	t.Parallel()

	crdDir := filepath.Join("..", "..", "config", "crd", "bases")
	outDir := t.TempDir()

	require.NoError(t, run(crdDir, outDir))

	schemaFile, err := os.Open(filepath.Join(outDir, "ledger.formance.com_v1alpha1_cluster.json"))
	require.NoError(t, err)
	defer func() { require.NoError(t, schemaFile.Close()) }()

	schemaDoc, err := jsonschema.UnmarshalJSON(schemaFile)
	require.NoError(t, err)

	compiler := jsonschema.NewCompiler()
	require.NoError(t, compiler.AddResource("ledger.formance.com_v1alpha1_cluster.json", schemaDoc))

	sch, err := compiler.Compile("ledger.formance.com_v1alpha1_cluster.json")
	require.NoError(t, err)

	cases := []struct {
		name    string
		doc     string
		wantErr bool
	}{
		{name: "empty object", doc: `{}`, wantErr: true},
		{
			name:    "wrong apiVersion and kind",
			doc:     `{"apiVersion":"other.example/v9","kind":"Unrelated","spec":{}}`,
			wantErr: true,
		},
		{
			name:    "correct identity",
			doc:     `{"apiVersion":"ledger.formance.com/v1alpha1","kind":"Cluster","metadata":{"name":"x"},"spec":{}}`,
			wantErr: false,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			instance, err := jsonschema.UnmarshalJSON(strings.NewReader(tc.doc))
			require.NoError(t, err)

			err = sch.Validate(instance)
			if tc.wantErr {
				require.Error(t, err, "expected %s to be rejected", tc.doc)
			} else {
				require.NoError(t, err, "expected %s to validate", tc.doc)
			}
		})
	}
}

// TestRunDoesNotEnforceCELRules documents a known, deliberate limitation:
// x-kubernetes-validations (CEL) rules are preserved in the exported schema
// for reference but are not translated into portable JSON Schema
// constraints, so a draft-04 validator does not enforce them. This fragment
// violates both CEL rules on PostgresMirrorSource (ledger_crd_types.go):
// passwordFrom and awsIamAuth are mutually exclusive, and awsIamAuth requires
// a TLS sslMode. Kubernetes admission remains the authoritative check for
// these rules. If this test starts failing because the fragment is now
// rejected, update the README's "Known limitation" paragraph to match.
func TestRunDoesNotEnforceCELRules(t *testing.T) {
	t.Parallel()

	crdDir := filepath.Join("..", "..", "config", "crd", "bases")
	outDir := t.TempDir()

	require.NoError(t, run(crdDir, outDir))

	schemaFile, err := os.Open(filepath.Join(outDir, "ledger.formance.com_v1alpha1_ledger.spec.json"))
	require.NoError(t, err)
	defer func() { require.NoError(t, schemaFile.Close()) }()

	schemaDoc, err := jsonschema.UnmarshalJSON(schemaFile)
	require.NoError(t, err)

	compiler := jsonschema.NewCompiler()
	require.NoError(t, compiler.AddResource("ledger.formance.com_v1alpha1_ledger.spec.json", schemaDoc))

	sch, err := compiler.Compile("ledger.formance.com_v1alpha1_ledger.spec.json")
	require.NoError(t, err)

	instance, err := jsonschema.UnmarshalJSON(strings.NewReader(`{
		"clusterRef": "my-cluster",
		"name": "my-ledger",
		"mirrorSource": {
			"postgres": {
				"host": "db.example.com",
				"user": "ledger",
				"database": "ledger",
				"sslMode": "disable",
				"passwordFrom": {"name": "pg-secret", "key": "password"},
				"awsIamAuth": {"region": "eu-west-1"}
			}
		}
	}`))
	require.NoError(t, err)

	require.NoError(t, sch.Validate(instance),
		"CEL-only rules (mutually exclusive auth, TLS-with-IAM) are not enforced by the exported schema")
}

func TestEnforceAdditionalPropertiesFalse(t *testing.T) {
	t.Parallel()

	doc := map[string]any{
		"type": "object",
		"properties": map[string]any{
			"strictChild": map[string]any{
				"type": "object",
				"properties": map[string]any{
					"name": map[string]any{"type": "string"},
				},
			},
			"mapField": map[string]any{
				"type":                 "object",
				"additionalProperties": map[string]any{"type": "string"},
			},
			"openField": map[string]any{
				"type":                                 "object",
				"x-kubernetes-preserve-unknown-fields": true,
				"properties": map[string]any{
					"anything": map[string]any{"type": "string"},
				},
			},
			"opaque": map[string]any{"type": "object"},
			"list": map[string]any{
				"type": "array",
				"items": map[string]any{
					"type": "object",
					"properties": map[string]any{
						"key": map[string]any{"type": "string"},
					},
				},
			},
		},
	}

	enforceAdditionalPropertiesFalse(doc)

	properties := doc["properties"].(map[string]any)

	require.Equal(t, false, doc["additionalProperties"])
	require.Equal(t, false, properties["strictChild"].(map[string]any)["additionalProperties"])
	require.Equal(t,
		map[string]any{"type": "string"},
		properties["mapField"].(map[string]any)["additionalProperties"],
	)
	require.NotContains(t, properties["openField"].(map[string]any), "additionalProperties")
	require.NotContains(t, properties["opaque"].(map[string]any), "additionalProperties")
	require.Equal(t,
		false,
		properties["list"].(map[string]any)["items"].(map[string]any)["additionalProperties"],
	)
}

func TestRunFailsWhenNoStorageVersion(t *testing.T) {
	t.Parallel()

	crdDir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(crdDir, "no-storage.yaml"), []byte(noStorageVersionCRD), 0o644))

	err := run(crdDir, t.TempDir())
	require.ErrorContains(t, err, "no version marked as storage version")
}

func TestRunPrunesStaleSchemaFiles(t *testing.T) {
	t.Parallel()

	crdDir := filepath.Join("..", "..", "config", "crd", "bases")
	outDir := t.TempDir()

	stalePath := filepath.Join(outDir, "ledger.formance.com_v1alpha1_doesnotexist.json")
	require.NoError(t, os.WriteFile(stalePath, []byte("{}"), 0o644))

	require.NoError(t, run(crdDir, outDir))

	require.NoFileExists(t, stalePath)
	require.FileExists(t, filepath.Join(outDir, "ledger.formance.com_v1alpha1_cluster.json"))
}

const noStorageVersionCRD = `
apiVersion: apiextensions.k8s.io/v1
kind: CustomResourceDefinition
metadata:
  name: widgets.example.com
spec:
  group: example.com
  names:
    kind: Widget
    plural: widgets
  scope: Namespaced
  versions:
    - name: v1alpha1
      served: true
      storage: false
      schema:
        openAPIV3Schema:
          type: object
          properties:
            spec:
              type: object
`
