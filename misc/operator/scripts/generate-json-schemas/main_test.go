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
