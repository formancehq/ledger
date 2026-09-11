package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRunGeneratesSchemasFromCRDs(t *testing.T) {
	t.Parallel()

	crdDir := filepath.Join("..", "..", "config", "crd", "bases")
	outDir := t.TempDir()

	require.NoError(t, run(crdDir, outDir))

	expected := []string{
		"v1alpha1_cluster.json",
		"v1alpha1_cluster.spec.json",
		"v1alpha1_ledger.json",
		"v1alpha1_ledger.spec.json",
		"v1alpha1_backup.json",
		"v1alpha1_backup.spec.json",
		"v1alpha1_backuprun.json",
		"v1alpha1_backuprun.spec.json",
		"v1alpha1_credentials.json",
		"v1alpha1_credentials.spec.json",
	}

	for _, name := range expected {
		path := filepath.Join(outDir, name)
		require.FileExists(t, path)

		data, err := os.ReadFile(path)
		require.NoError(t, err)

		var doc map[string]any
		require.NoError(t, json.Unmarshal(data, &doc))
		require.Equal(t, jsonSchemaDraft04, doc["$schema"])
		require.NotEmpty(t, doc["type"])
	}
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

	stalePath := filepath.Join(outDir, "v1alpha1_doesnotexist.json")
	require.NoError(t, os.WriteFile(stalePath, []byte("{}"), 0o644))

	require.NoError(t, run(crdDir, outDir))

	require.NoFileExists(t, stalePath)
	require.FileExists(t, filepath.Join(outDir, "v1alpha1_cluster.json"))
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
